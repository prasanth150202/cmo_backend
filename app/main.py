from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
from app.api.api_v1.api import api_router
from app.core.config import settings


def _auto_sync_recent() -> None:
    """
    Background job: re-pull the last 3 days for all mapped META accounts.
    Runs every hour so the dashboard always shows fresh data without manual syncing.
    Meta insights are delayed up to 48 h and updated retroactively, so we force
    re-pull (skip_existing=False) for this short window.
    """
    try:
        from datetime import datetime, timedelta
        from app.db.supabase import supabase
        from app.services.ingest import IngestService
        from app.core.config import settings as _settings

        if not _settings.META_SYSTEM_USER_TOKEN:
            return

        date_to   = datetime.now().strftime("%Y-%m-%d")
        date_from = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")

        accts_resp = supabase.table("brand_accounts").select("account_id, platform").execute()
        meta_accounts = [
            a["account_id"] for a in (accts_resp.data or [])
            if a.get("platform", "").upper() == "META"
        ]

        for account_id in meta_accounts:
            IngestService.sync_daily_metrics(account_id, date_from, date_to, skip_existing=False)
            IngestService.sync_campaign_daily_metrics(account_id, date_from, date_to, skip_existing=False)

        if meta_accounts:
            print(f"[scheduler] auto-sync done for {len(meta_accounts)} accounts ({date_from} → {date_to})")
    except Exception as e:
        print(f"[scheduler] auto-sync error: {e}")


scheduler = BackgroundScheduler(timezone="UTC")
scheduler.add_job(_auto_sync_recent, "interval", hours=1, id="auto_sync_recent", replace_existing=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.start()
    print("[scheduler] started — auto-sync runs every hour")
    yield
    scheduler.shutdown(wait=False)
    print("[scheduler] stopped")


app = FastAPI(
    title=settings.PROJECT_NAME,
    openapi_url=f"{settings.API_V1_STR}/openapi.json",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router, prefix=settings.API_V1_STR)


@app.get("/health")
def health_check():
    return {"status": "ok", "version": "1.0.0"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
