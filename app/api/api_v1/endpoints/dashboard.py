from typing import Any, List, Dict, Optional
from fastapi import APIRouter, HTTPException, Query
from app.db.supabase import supabase
from app.services.mock_data import get_mock_meta_entities
from app.services.rules.executor import executor

router = APIRouter()


@router.get("/summary", response_model=dict)
def get_dashboard_summary(
    date_from: Optional[str] = Query(default=None),
    date_to: Optional[str] = Query(default=None),
) -> Any:
    """
    Consolidated performance summary for the selected date range.
    Pulls from Meta Insights API when token is configured; falls back to Supabase.
    """
    try:
        from app.api.api_v1.endpoints.analytics import _fetch_daily_from_meta, _default_dates
        if not date_from or not date_to:
            date_from, date_to = _default_dates()

        daily = _fetch_daily_from_meta(date_from, date_to)

        if daily:
            total_spend = round(sum(r["spend"] for r in daily), 2)
            total_revenue = round(sum(r["revenue"] for r in daily), 2)
            total_conversions = round(sum(r["conversions"] for r in daily), 0)
            roas = round(total_revenue / total_spend, 2) if total_spend > 0 else 0
        else:
            resp = supabase.table("performance_metrics").select("*").execute()
            metrics = resp.data or []
            total_spend = sum(float(m.get("spend") or 0) for m in metrics)
            total_revenue = sum(float(m.get("revenue") or 0) for m in metrics)
            total_conversions = sum(float(m.get("conversions") or 0) for m in metrics)
            roas = round(total_revenue / total_spend, 2) if total_spend > 0 else 0

        # Rule engine runs on mock entities until real EntityContext is built from Meta sync
        entities = get_mock_meta_entities()
        suggestions = executor.process_entities(entities)

        return {
            "metrics": {
                "total_spend": round(total_spend, 2),
                "total_revenue": round(total_revenue, 2),
                "roas": roas,
                "total_conversions": round(total_conversions, 0),
            },
            "suggestions_count": len(suggestions),
            "suggestions": suggestions,
            "entities_count": len(daily) if daily else 0,
            "channel": "META",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/live-state")
def get_live_state() -> List[Dict[str, Any]]:
    """All performance_metrics rows as-is from Supabase."""
    try:
        resp = supabase.table("performance_metrics").select("*").order("spend", desc=True).execute()
        return resp.data or []
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sync")
def trigger_sync(ad_account_id: str) -> Any:
    """Campaign-level sync for rule engine (populates performance_metrics)."""
    from app.services.ingest import ingest_service
    try:
        result = ingest_service.sync_meta_accounts(ad_account_id)
        return {"status": "success", **result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sync-history")
def sync_history(days: int = 90) -> Any:
    """
    Pull `days` of history from Meta for all mapped accounts.
    Syncs both daily_metrics (account-level) and campaign_daily_metrics (campaign-level).
    """
    from app.services.ingest import IngestService
    from datetime import datetime, timedelta
    try:
        date_to   = datetime.now().strftime("%Y-%m-%d")
        date_from = (datetime.now() - timedelta(days=days - 1)).strftime("%Y-%m-%d")

        # Step 1: account-level daily
        account_result = IngestService.sync_all_accounts_daily(date_from, date_to)

        # Step 2: campaign-level daily for each account
        try:
            accts_resp = supabase.table("brand_accounts").select("account_id, platform").execute()
            meta_accounts = [
                a["account_id"] for a in (accts_resp.data or [])
                if a.get("platform", "").upper() == "META"
            ]
        except Exception:
            meta_accounts = []

        campaign_results = []
        for account_id in meta_accounts:
            r = IngestService.sync_campaign_daily_metrics(account_id, date_from, date_to, skip_existing=True)
            campaign_results.append(r)

        return {
            "status":    "success",
            "date_from": date_from,
            "date_to":   date_to,
            "account_level":  account_result,
            "campaign_level": {
                "accounts_synced":   len(campaign_results),
                "total_rows_synced": sum(r.get("rows_synced", 0) for r in campaign_results),
            },
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/sync-status")
def sync_status() -> Any:
    """
    Returns the latest sync job per account so the UI can show progress indicators.
    Ordered by most recent first.
    """
    try:
        resp = (
            supabase.table("sync_jobs")
            .select("*")
            .order("created_at", desc=True)
            .limit(50)
            .execute()
        )
        jobs = resp.data or []
        # Deduplicate: keep only the latest job per account
        seen, latest = set(), []
        for j in jobs:
            if j["account_id"] not in seen:
                seen.add(j["account_id"])
                latest.append(j)
        return latest
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sync-recent")
def sync_recent() -> Any:
    """
    Pull the last 3 days from Meta and upsert into daily_metrics + campaign_daily_metrics.
    Always force re-pulls (skip_existing=False) because Meta's insights data is delayed
    up to 48h and retroactively updated — skipping existing rows would miss the corrections.
    """
    from app.services.ingest import IngestService
    from datetime import datetime, timedelta
    try:
        date_to = datetime.now().strftime("%Y-%m-%d")
        date_from = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")

        # Fetch all META accounts
        accts_resp = supabase.table("brand_accounts").select("account_id, platform").execute()
        meta_accounts = [
            a["account_id"] for a in (accts_resp.data or [])
            if a.get("platform", "").upper() == "META"
        ]

        account_results = []
        campaign_results = []
        for account_id in meta_accounts:
            # Force re-pull account-level daily (skip_existing=False)
            r1 = IngestService.sync_daily_metrics(account_id, date_from, date_to, skip_existing=False)
            account_results.append(r1)
            # Force re-pull campaign-level daily (skip_existing=False)
            r2 = IngestService.sync_campaign_daily_metrics(account_id, date_from, date_to, skip_existing=False)
            campaign_results.append(r2)

        return {
            "status": "success",
            "date_from": date_from,
            "date_to": date_to,
            "accounts_synced": len(meta_accounts),
            "account_rows_synced": sum(r.get("rows_synced", 0) for r in account_results),
            "campaign_rows_synced": sum(r.get("rows_synced", 0) for r in campaign_results),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/narrative")
async def get_suggestion_narrative(suggestion: dict) -> Any:
    """AI-generated reasoning for a specific suggestion."""
    from app.services.ai import ai_service
    try:
        narrative = await ai_service.get_suggestion_narrative({"rule_triggers": suggestion})
        if not narrative:
            return {"suggestion_text": "AI narrative unavailable.", "reasoning": "Check API keys."}
        return narrative
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
