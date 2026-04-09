"""
Meta Webhooks endpoint.

Meta webhooks notify on ad object changes (campaigns/adsets/ads paused, budget edited, etc.)
NOT on spend/ROAS — those are pull-only. So on any change for account X, we re-sync
the last 3 days for account X only (Meta's data can lag up to 48h).

Setup in Meta App Dashboard → Webhooks:
  Object: adaccount
  Callback URL: https://your-domain.com/api/v1/webhooks/meta
  Verify Token: value of META_WEBHOOK_VERIFY_TOKEN in .env
  Fields: subscribe to: campaigns, adsets, ads (or adaccount)
"""

from datetime import datetime, timedelta
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request, Response
from app.core.config import settings

router = APIRouter()


def _sync_account_recent(account_id: str) -> None:
    """Re-sync the last 3 days for a single account. Meta data can lag up to 48h."""
    from app.services.ingest import IngestService
    from app.db.supabase import supabase
    date_to   = datetime.now().strftime("%Y-%m-%d")
    date_from = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
    clean_id  = account_id.replace("act_", "")
    try:
        job_resp = supabase.table("sync_jobs").insert({
            "account_id": clean_id,
            "status":     "pending",
            "date_from":  date_from,
            "date_to":    date_to,
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
    print(f"[webhook] re-synced {account_id}: account={r1} campaigns={r2}")


@router.get("/meta")
def verify_meta_webhook(
    hub_mode: str = Query(alias="hub.mode", default=""),
    hub_verify_token: str = Query(alias="hub.verify_token", default=""),
    hub_challenge: str = Query(alias="hub.challenge", default=""),
) -> Response:
    """Meta verification handshake — echoes hub.challenge when token matches."""
    if hub_mode == "subscribe" and hub_verify_token == settings.META_WEBHOOK_VERIFY_TOKEN:
        return Response(content=hub_challenge, media_type="text/plain")
    raise HTTPException(status_code=403, detail="Webhook verification failed")


@router.post("/meta")
async def receive_meta_webhook(request: Request, background_tasks: BackgroundTasks) -> dict:
    """
    Receive Meta webhook events. Extracts the affected account_id(s) from the payload
    and triggers a background re-sync for each one individually.

    Meta payload shape for adaccount subscriptions:
      { "object": "adaccount",
        "entry": [{ "id": "act_123456789", "changes": [...] }] }
    """
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    entries = payload.get("entry", [])
    synced_accounts = []

    for entry in entries:
        raw_id = entry.get("id", "")  # e.g. "act_123456789" or "123456789"
        # Normalise: strip act_ for storage consistency
        account_id = raw_id.replace("act_", "")

        if account_id and account_id not in synced_accounts:
            synced_accounts.append(account_id)
            background_tasks.add_task(_sync_account_recent, account_id)
            print(f"[webhook] queued re-sync for account {account_id}")

    return {"status": "ok", "accounts_queued": synced_accounts}
