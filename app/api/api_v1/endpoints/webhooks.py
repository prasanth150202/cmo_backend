"""
Meta Ads Webhook endpoint.

Meta webhooks fire on STRUCTURAL changes (campaign paused, budget edited,
ad status changed, creative updated, spending limit hit) — NOT on
spend/ROAS/impressions. Those are pull-only by Meta's design.

On any structural change → we queue a background re-sync of the last 3 days
for the affected ad account (Meta data can lag up to 48 h).

──────────────────────────────────────────────────────────────────────────────
SETUP  (Meta App Dashboard → Webhooks)
  Object:        adaccount
  Callback URL:  https://cmo-backend-2v2n.onrender.com/api/v1/webhooks/meta
  Verify Token:  value of META_WEBHOOK_VERIFY_TOKEN in .env
  Fields to subscribe:
    campaigns
    adsets
    ads
    ad_creatives
    account_spending_limit_reached
    bidding_update

Signature verification: HMAC-SHA256 via X-Hub-Signature-256 using META_APP_SECRET.
──────────────────────────────────────────────────────────────────────────────
"""

import hashlib
import hmac
import json
from datetime import datetime, timedelta

from fastapi import APIRouter, BackgroundTasks, Header, HTTPException, Query, Request, Response

from app.core.config import settings

router = APIRouter()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _verify_meta_signature(body: bytes, sig_header: str) -> bool:
    """Verify X-Hub-Signature-256 from Meta using META_APP_SECRET."""
    if not settings.META_APP_SECRET or not sig_header:
        return False
    expected = "sha256=" + hmac.new(
        settings.META_APP_SECRET.encode(), body, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, sig_header)


def _log_event(payload: dict, account_id: str, obj_type: str) -> None:
    """Write raw event to webhook_events for audit / replay."""
    try:
        from app.db.supabase import supabase
        supabase.table("webhook_events").insert({
            "source": "meta",
            "topic": obj_type,
            "account_id": account_id,
            "payload": json.dumps(payload),
            "received_at": datetime.utcnow().isoformat(),
        }).execute()
    except Exception as e:
        print(f"[webhook/meta] log_event error: {e}")


def _sync_account_recent(account_id: str) -> None:
    """Re-sync the last 3 days for one ad account."""
    from app.db.supabase import supabase
    from app.services.ingest import IngestService

    date_to = datetime.now().strftime("%Y-%m-%d")
    date_from = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
    clean_id = account_id.replace("act_", "")

    try:
        job_resp = supabase.table("sync_jobs").insert({
            "account_id": clean_id,
            "status": "pending",
            "date_from": date_from,
            "date_to": date_to,
        }).execute()
        job_id = job_resp.data[0]["id"] if job_resp.data else None
    except Exception:
        job_id = None

    r1 = IngestService.sync_daily_metrics(
        account_id, date_from, date_to, job_id=job_id, skip_existing=False
    )
    r2 = IngestService.sync_campaign_daily_metrics(
        account_id, date_from, date_to, skip_existing=False
    )
    print(f"[webhook/meta] re-synced {account_id}: account={r1} campaigns={r2}")


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/meta")
def verify_meta_webhook(
    hub_mode: str = Query(alias="hub.mode", default=""),
    hub_verify_token: str = Query(alias="hub.verify_token", default=""),
    hub_challenge: str = Query(alias="hub.challenge", default=""),
) -> Response:
    """
    Meta verification handshake.
    Meta sends a GET with hub.mode=subscribe and hub.verify_token.
    We echo back hub.challenge if the token matches.
    """
    if hub_mode == "subscribe" and hub_verify_token == settings.META_WEBHOOK_VERIFY_TOKEN:
        return Response(content=hub_challenge, media_type="text/plain")
    raise HTTPException(status_code=403, detail="Webhook verification failed")


@router.post("/meta")
async def receive_meta_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    x_hub_signature_256: str = Header(default=""),
) -> dict:
    """
    Receive Meta webhook events.

    Supported objects: adaccount, campaign, adset, ad

    Meta payload shape:
      {
        "object": "adaccount",
        "entry": [
          {
            "id": "act_123456789",
            "changes": [
              { "field": "campaigns", "value": { "campaign_id": "...", "verb": "UPDATE" } }
            ]
          }
        ]
      }

    For campaign/adset/ad objects the account_id lives inside changes[].value.
    """
    body = await request.body()

    # Signature check — only enforced when APP_SECRET is set (local dev can skip)
    if settings.META_APP_SECRET:
        if not _verify_meta_signature(body, x_hub_signature_256):
            raise HTTPException(status_code=403, detail="Invalid Meta webhook signature")

    try:
        payload = json.loads(body)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    obj_type = payload.get("object", "")   # adaccount | campaign | adset | ad
    entries = payload.get("entry", [])
    queued: list[str] = []

    for entry in entries:
        raw_id = entry.get("id", "")
        account_id = raw_id.replace("act_", "")

        # campaign / adset / ad entries don't include account_id at the top level
        if not account_id:
            for change in entry.get("changes", []):
                aid = change.get("value", {}).get("account_id", "")
                if aid:
                    account_id = aid.replace("act_", "")
                    break

        if account_id and account_id not in queued:
            queued.append(account_id)
            _log_event(entry, account_id, obj_type)
            background_tasks.add_task(_sync_account_recent, account_id)
            print(f"[webhook/meta] queued re-sync for {account_id} (object={obj_type})")

    return {"status": "ok", "object": obj_type, "accounts_queued": queued}
