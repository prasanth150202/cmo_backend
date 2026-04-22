from typing import Any, List, Optional
from datetime import datetime, timedelta
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from pydantic import BaseModel
from app.db.supabase import supabase


def _pull_full_history(account_id: str, job_id: str) -> None:
    """
    Background task triggered when a new account is mapped to a brand.
    Pulls 3 years of history in order:
      1. Account-level daily (90-day chunks) → daily_metrics
      2. Campaign-level daily (30-day chunks) → campaign_daily_metrics
    Sequential to stay within Meta rate limits.
    """
    from app.services.ingest import IngestService
    date_to   = datetime.now().strftime("%Y-%m-%d")
    date_from = (datetime.now() - timedelta(days=3 * 365)).strftime("%Y-%m-%d")

    # Step 1: account-level daily
    r1 = IngestService.sync_daily_metrics(
        account_id, date_from, date_to, job_id=job_id, skip_existing=True
    )
    print(f"[brand-map] account-level done for {account_id}: {r1}")

    # Step 2: campaign-level daily (starts after account sync finishes)
    r2 = IngestService.sync_campaign_daily_metrics(
        account_id, date_from, date_to, skip_existing=True
    )
    print(f"[brand-map] campaign-level done for {account_id}: {r2}")

router = APIRouter()

# ── Schemas ──────────────────────────────────────────────────────────────────

class BrandCreate(BaseModel):
    name: str
    color: str = "#6366f1"
    industry: str = ""
    target_roas: float = 3.0
    monthly_budget_cap: float = 0.0
    logo_url: Optional[str] = None
    website_url: Optional[str] = None

class BrandUpdate(BaseModel):
    name: Optional[str] = None
    color: Optional[str] = None
    industry: Optional[str] = None
    target_roas: Optional[float] = None
    monthly_budget_cap: Optional[float] = None
    logo_url: Optional[str] = None
    website_url: Optional[str] = None

class AccountMap(BaseModel):
    brand_id: str
    platform: str  # META | GOOGLE | DV360
    account_id: str
    account_name: str = ""

# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/")
def list_brands() -> Any:
    """List all brands with their mapped accounts."""
    try:
        brands_resp = supabase.table("brands").select("*").order("created_at").execute()
        brands = brands_resp.data or []

        accounts_resp = supabase.table("brand_accounts").select("*").execute()
        accounts = accounts_resp.data or []

        # Nest accounts under each brand
        for b in brands:
            b["accounts"] = [a for a in accounts if a["brand_id"] == b["id"]]

        return brands
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/")
def create_brand(brand: BrandCreate) -> Any:
    """Create a new brand."""
    try:
        resp = supabase.table("brands").insert(brand.dict()).execute()
        return resp.data[0]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/{brand_id}")
def update_brand(brand_id: str, brand: BrandUpdate) -> Any:
    """Update an existing brand."""
    try:
        # Exclude unset fields so we only update what's provided
        update_data = brand.dict(exclude_unset=True)
        if not update_data:
            raise HTTPException(status_code=400, detail="No update data provided")

        resp = supabase.table("brands").update(update_data).eq("id", brand_id).execute()
        if not resp.data:
            raise HTTPException(status_code=404, detail="Brand not found")
        return resp.data[0]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/{brand_id}")
def delete_brand(brand_id: str) -> Any:
    """Delete a brand and all its mapped accounts."""
    try:
        supabase.table("brands").delete().eq("id", brand_id).execute()
        return {"status": "deleted", "brand_id": brand_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/accounts")
def map_account(account: AccountMap, background_tasks: BackgroundTasks) -> Any:
    """Map an ad account to a brand and immediately kick off a full historical pull."""
    try:
        resp = supabase.table("brand_accounts").upsert(account.dict()).execute()

        if account.platform.upper() == "META":
            # Create a tracked job so the UI can show sync progress
            date_to   = datetime.now().strftime("%Y-%m-%d")
            date_from = (datetime.now() - timedelta(days=3 * 365)).strftime("%Y-%m-%d")
            clean_id  = account.account_id.replace("act_", "")
            job_resp  = supabase.table("sync_jobs").insert({
                "account_id": clean_id,
                "status":     "pending",
                "date_from":  date_from,
                "date_to":    date_to,
            }).execute()
            job_id = job_resp.data[0]["id"] if job_resp.data else None
            background_tasks.add_task(_pull_full_history, account.account_id, job_id)

        return resp.data[0]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/accounts/{account_id}")
def unmap_account(account_id: str) -> Any:
    """Remove an account mapping from a brand."""
    try:
        supabase.table("brand_accounts").delete().eq("id", account_id).execute()
        return {"status": "removed"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/overview")
def get_brands_overview(
    date_from: Optional[str] = Query(default=None),
    date_to: Optional[str] = Query(default=None),
) -> Any:
    """
    All brands with performance metrics for the selected date range.
    Pulls from Meta API when token configured; falls back to Supabase.
    """
    from app.api.api_v1.endpoints.analytics import _fetch_account_totals, _default_dates

    try:
        brands_resp = supabase.table("brands").select("*").order("created_at").execute()
        brands = brands_resp.data or []
        accounts_resp = supabase.table("brand_accounts").select("*").execute()
        accounts = accounts_resp.data or []
    except Exception:
        return []

    if not date_from or not date_to:
        date_from, date_to = _default_dates()

    # Try Meta API first — returns per-account totals
    account_totals = _fetch_account_totals(date_from, date_to)

    # Fallback: Supabase performance_metrics
    supabase_metrics: list = []
    if not account_totals:
        try:
            resp = supabase.table("performance_metrics").select("*").execute()
            supabase_metrics = resp.data or []
        except Exception:
            supabase_metrics = []

    result = []
    for b in brands:
        brand_accounts = [a for a in accounts if a["brand_id"] == b["id"]]
        # Normalize: strip act_ prefix so IDs match daily_metrics storage format
        account_ids = {a["account_id"].replace("act_", "") for a in brand_accounts}

        target_roas = float(b.get("target_roas") or 3.0)

        if account_totals:
            spend = round(sum(account_totals.get(aid, {}).get("spend", 0) for aid in account_ids), 2)
            revenue = round(sum(account_totals.get(aid, {}).get("revenue", 0) for aid in account_ids), 2)
            conversions = round(sum(account_totals.get(aid, {}).get("conversions", 0) for aid in account_ids), 0)
            clicks = sum(account_totals.get(aid, {}).get("clicks", 0) for aid in account_ids)
        else:
            has_account_id = supabase_metrics and "account_id" in supabase_metrics[0]
            if has_account_id:
                brand_metrics = [m for m in supabase_metrics if m.get("account_id") in account_ids]
            else:
                brand_metrics = supabase_metrics if brand_accounts else []
            spend = round(sum(float(m.get("spend") or 0) for m in brand_metrics), 2)
            revenue = round(sum(float(m.get("revenue") or 0) for m in brand_metrics), 2)
            conversions = round(sum(float(m.get("conversions") or 0) for m in brand_metrics), 0)
            clicks = sum(int(m.get("clicks") or 0) for m in brand_metrics)

        roas = round(revenue / spend, 2) if spend > 0 else 0
        cvr = round(conversions / clicks * 100, 2) if clicks > 0 else 0
        score = min(100, int((roas / target_roas) * 60 + min(cvr * 10, 40))) if roas > 0 else 0

        result.append({
            "brand_id": b["id"],
            "brand_name": b["name"],
            "brand_color": b.get("color", "#6366f1"),
            "logo_url": b.get("logo_url"),
            "website_url": b.get("website_url"),
            "industry": b.get("industry", ""),
            "target_roas": target_roas,
            "monthly_budget_cap": float(b.get("monthly_budget_cap") or 0),
            "accounts_count": len(brand_accounts),
            "metrics": {
                "spend": spend,
                "revenue": revenue,
                "roas": roas,
                "conversions": conversions,
                "score": score,
            },
        })
    return result


@router.get("/{brand_id}/detail")
def get_brand_detail(
    brand_id: str,
    date_from: Optional[str] = Query(default=None),
    date_to: Optional[str] = Query(default=None),
    background_tasks: BackgroundTasks = None,
) -> Any:
    """
    Full brand detail: today's KPIs, conversion funnel, 7-day daily breakdown,
    per-account scorecard. Reads from daily_metrics + targeted Meta funnel pull.
    """
    from app.api.api_v1.endpoints.analytics import _default_dates
    from collections import defaultdict

    today = datetime.now().strftime("%Y-%m-%d")
    if not date_from or not date_to:
        date_from, date_to = _default_dates()

    from datetime import datetime, timedelta

    try:
        brand = (
            supabase.table("brands")
            .select("id, name, color, logo_url, website_url, industry, target_roas")
            .eq("id", brand_id)
            .single()
            .execute()
        ).data
        if not brand:
            raise HTTPException(status_code=404, detail="Brand not found")

        # ── Fetch accounts for this brand ──────────────────────────────────────
        accounts = (
            supabase.table("brand_accounts")
            .select("account_id, platform, account_name")
            .eq("brand_id", brand_id)
            .execute()
        ).data or []
        account_ids = [a["account_id"].replace("act_", "") for a in accounts]

        if not date_from or not date_to:
            from app.api.api_v1.endpoints.analytics import _default_dates
            date_from, date_to = _default_dates()

        if not account_ids:
            return {
                "brand": brand, "summary": {}, "daily": [], 
                "funnel": {}, "scorecard": [], "campaigns": [],
                "date_from": date_from, "date_to": date_to, "last_synced": None
            }

        # ── Daily Metrics (Aggregated over accounts) ──────────────────────────
        # Include synced_at to check for staleness
        daily_resp = (
            supabase.table("daily_metrics")
            .select("date, spend, revenue, impressions, clicks, conversions, ctr_sum, ctr_n, account_id, synced_at")
            .in_("account_id", list(account_ids))
            .gte("date", date_from)
            .lte("date", date_to)
            .order("date", desc=False)
            .execute()
        )
        daily_rows = daily_resp.data or []

        # Check for staleness (6 hours)
        latest_sync = None
        for r in daily_rows:
            st = r.get("synced_at")
            if st:
                if latest_sync is None or st > latest_sync:
                    latest_sync = st
        
        is_stale = False
        if latest_sync:
            ls_dt = datetime.fromisoformat(latest_sync.replace("Z", "+00:00"))
            if datetime.now(ls_dt.tzinfo) - ls_dt > timedelta(hours=6):
                is_stale = True

        # If data is missing OR stale, trigger background sync
        if (not daily_rows or is_stale) and background_tasks is not None:
            from app.services.ingest import IngestService
            for acct in accounts:
                clean_id = acct["account_id"].replace("act_", "")
                background_tasks.add_task(
                    IngestService.sync_daily_metrics,
                    clean_id, date_from, date_to, None, not is_stale,
                )
                background_tasks.add_task(
                    IngestService.sync_campaign_daily_metrics,
                    clean_id, date_from, date_to, None, not is_stale,
                )

        daily_agg_dict: dict = {}
        for row in daily_rows:
            date = str(row["date"])
            if date not in daily_agg_dict:
                daily_agg_dict[date] = {"date": date, "spend": 0.0, "revenue": 0.0, "impressions": 0, "clicks": 0, "conversions": 0.0, "ctr_sum": 0.0, "ctr_n": 0}
            
            daily_agg_dict[date]["spend"]       += float(row["spend"] or 0)
            daily_agg_dict[date]["revenue"]     += float(row["revenue"] or 0)
            daily_agg_dict[date]["impressions"] += int(row["impressions"] or 0)
            daily_agg_dict[date]["clicks"]      += int(row["clicks"] or 0)
            daily_agg_dict[date]["conversions"] += float(row["conversions"] or 0)
            daily_agg_dict[date]["ctr_sum"]     += float(row["ctr_sum"] or 0)
            daily_agg_dict[date]["ctr_n"]       += int(row["ctr_n"] or 0)

        daily_agg = []
        for date in sorted(daily_agg_dict.keys()):
            row = daily_agg_dict[date]
            daily_agg.append({
                "date":        date,
                "spend":       round(row["spend"], 2),
                "revenue":     round(row["revenue"], 2),
                "roas":        round(row["revenue"] / row["spend"], 2) if row["spend"] > 0 else 0.0,
                "impressions": row["impressions"],
                "clicks":      row["clicks"],
                "conversions": round(row["conversions"], 1),
                "ctr":         round(row["ctr_sum"] / row["ctr_n"], 2) if row["ctr_n"] > 0 else 0.0,
            })

        # ── Aggregate over the full selected date range ────────────────────────
        total_spend       = round(sum(float(r["spend"]) for r in daily_agg), 2)
        total_revenue     = round(sum(float(r["revenue"]) for r in daily_agg), 2)
        total_roas        = round(total_revenue / total_spend, 2) if total_spend > 0 else 0.0
        total_impressions = sum(int(r["impressions"]) for r in daily_agg)
        total_clicks      = sum(int(r["clicks"]) for r in daily_agg)
        total_conversions = round(sum(float(r["conversions"]) for r in daily_agg), 1)
        ctr_vals          = [r["ctr"] for r in daily_agg if r["ctr"] > 0]
        total_ctr         = round(sum(ctr_vals) / len(ctr_vals), 2) if ctr_vals else 0.0

        # ── Funnel aggregated over the full date range (from DB) ───────────────
        funnel = _fetch_brand_funnel(list(account_ids), date_from, date_to)

        # ── Per-account scorecard ──────────────────────────────────────────────
        by_account: dict = defaultdict(lambda: {"spend": 0.0, "revenue": 0.0, "conversions": 0.0, "impressions": 0, "clicks": 0})
        for r in daily_rows:
            aid = str(r.get("account_id", ""))
            by_account[aid]["spend"]       += float(r.get("spend") or 0)
            by_account[aid]["revenue"]     += float(r.get("revenue") or 0)
            by_account[aid]["conversions"] += float(r.get("conversions") or 0)
            by_account[aid]["impressions"] += int(r.get("impressions") or 0)
            by_account[aid]["clicks"]      += int(r.get("clicks") or 0)

        scorecard = []
        for acct in accounts:
            aid  = acct["account_id"].replace("act_", "")
            m    = by_account.get(aid, {})
            sp   = round(m.get("spend", 0), 2)
            rev  = round(m.get("revenue", 0), 2)
            conv = round(m.get("conversions", 0), 1)
            clk  = m.get("clicks", 0)
            roas = round(rev / sp, 2)   if sp   > 0 else 0.0
            cvr  = round(conv / clk * 100, 2) if clk > 0 else 0.0
            cpa  = round(sp / conv, 2)  if conv > 0 else None
            # Score: 0-100 based on ROAS vs target, CVR, spend efficiency
            target_roas_val = float(brand.get("target_roas") or 3.0)
            score = min(100, int((roas / target_roas_val) * 60 + min(cvr * 10, 40))) if roas > 0 else 0
            scorecard.append({
                "account_id":   aid,
                "account_name": acct.get("account_name") or aid,
                "platform":     acct.get("platform", "META"),
                "spend":        sp,
                "revenue":      rev,
                "roas":         roas,
                "conversions":  conv,
                "cvr":          cvr,
                "cpa":          cpa,
                "atc":          funnel.get(aid, {}).get("atc", 0),
                "score":        score,
            })

        # ── Campaigns ─────────────────────────────────────────────────────────
        all_camps_resp = (
            supabase.table("campaigns")
            .select("id, name, status, account_id")
            .in_("account_id", list(account_ids))
            .execute()
        )
        all_camps = all_camps_resp.data or []

        campaign_ids_all = [c["id"] for c in all_camps]
        camp_agg: dict = defaultdict(lambda: {
            "campaign_name": "", "account_id": "",
            "spend": 0.0, "revenue": 0.0, "conversions": 0.0,
            "impressions": 0, "clicks": 0, "atc": 0.0, "checkout": 0.0,
            "ctr_sum": 0.0, "ctr_n": 0,
        })

        first_seen: dict = {}
        if campaign_ids_all:
            earliest_resp = (
                supabase.table("campaign_daily_metrics")
                .select("campaign_id, date")
                .in_("campaign_id", campaign_ids_all)
                .order("date", desc=False)
                .execute()
            )
            for r in (earliest_resp.data or []):
                cid = r["campaign_id"]
                d_val = str(r["date"])[:10]
                if cid not in first_seen or d_val < first_seen[cid]:
                    first_seen[cid] = d_val

            camp_metrics_resp = (
                supabase.table("campaign_daily_metrics")
                .select("campaign_id, campaign_name, account_id, spend, revenue, conversions, impressions, clicks, ctr, atc, checkout")
                .in_("campaign_id", campaign_ids_all)
                .gte("date", date_from)
                .lte("date", date_to)
                .execute()
            )
            for r in (camp_metrics_resp.data or []):
                cid = r["campaign_id"]
                camp_agg[cid]["campaign_name"] = r.get("campaign_name") or cid
                camp_agg[cid]["account_id"]    = r.get("account_id", "")
                camp_agg[cid]["spend"]        += float(r.get("spend") or 0)
                camp_agg[cid]["revenue"]      += float(r.get("revenue") or 0)
                camp_agg[cid]["conversions"]  += float(r.get("conversions") or 0)
                camp_agg[cid]["impressions"]  += int(r.get("impressions") or 0)
                camp_agg[cid]["clicks"]       += int(r.get("clicks") or 0)
                camp_agg[cid]["atc"]          += float(r.get("atc") or 0)
                camp_agg[cid]["checkout"]     += float(r.get("checkout") or 0)
                ctr_val = float(r.get("ctr") or 0)
                if ctr_val > 0:
                    camp_agg[cid]["ctr_sum"] += ctr_val
                    camp_agg[cid]["ctr_n"]   += 1

        campaigns = []
        for c in all_camps:
            cid = c["id"]
            cm   = camp_agg.get(cid, {})
            sp_val  = round(float(cm.get("spend", 0)), 2)
            rev_val = round(float(cm.get("revenue", 0)), 2)
            clk_val = int(cm.get("clicks", 0))
            conv_val = round(float(cm.get("conversions", 0)), 1)
            campaigns.append({
                "campaign_id":   cid,
                "status":        c.get("status", "UNKNOWN"),
                "campaign_name": cm.get("campaign_name") or c.get("name") or cid,
                "account_id":    c.get("account_id", ""),
                "created_at":    first_seen.get(cid),
                "spend":         sp_val,
                "revenue":       rev_val,
                "roas":          round(rev_val / sp_val, 2) if sp_val > 0 else 0.0,
                "conversions":   conv_val,
                "impressions":   int(cm.get("impressions", 0)),
                "clicks":        clk_val,
                "ctr":           round(cm["ctr_sum"] / cm["ctr_n"], 2) if cm.get("ctr_n", 0) > 0 else 0.0,
                "atc":           int(cm.get("atc", 0)),
                "checkout":      int(cm.get("checkout", 0)),
                "cvr":           round(conv_val / clk_val * 100, 2) if clk_val > 0 else 0.0,
                "cpa":           round(sp_val / conv_val, 2) if conv_val > 0 else None,
            })
        campaigns.sort(key=lambda x: (x["status"] != "ACTIVE", -x["spend"]))

        return {
            "brand": {
                "id":           brand["id"],
                "name":         brand["name"],
                "color":        brand.get("color", "#6366f1"),
                "logo_url":     brand.get("logo_url"),
                "website_url":  brand.get("website_url"),
                "industry":     brand.get("industry", ""),
                "target_roas":  float(brand.get("target_roas") or 3.0),
            },
            "summary": {
                "spend":       total_spend,
                "revenue":     total_revenue,
                "roas":        total_roas,
                "purchases":   total_conversions,
                "impressions": total_impressions,
                "clicks":      total_clicks,
                "ctr":         total_ctr,
                "atc":         sum(v.get("atc", 0) for v in funnel.values()),
                "atc_value":   sum(v.get("atc_value", 0) for v in funnel.values()),
                "checkout":    sum(v.get("checkout", 0) for v in funnel.values()),
            },
            "daily":    daily_agg,
            "funnel":   funnel,
            "scorecard": scorecard,
            "campaigns": campaigns,
            "date_from": date_from,
            "date_to":   date_to,
            "last_synced": latest_sync,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _fetch_brand_funnel(account_ids: list, date_from: str, date_to: str) -> dict:
    """
    Read funnel totals (ATC, checkout, purchase) from daily_metrics for the date range.
    Returns dict keyed by account_id with aggregated totals.
    """
    try:
        resp = (
            supabase.table("daily_metrics")
            .select("account_id, atc, atc_value, checkout, conversions, revenue")
            .in_("account_id", list(account_ids))
            .gte("date", date_from)
            .lte("date", date_to)
            .execute()
        )
        # Aggregate per account across all days in range
        from collections import defaultdict
        agg: dict = defaultdict(lambda: {"atc": 0.0, "atc_value": 0.0, "checkout": 0.0, "purchase": 0.0, "purchase_value": 0.0})
        for r in (resp.data or []):
            aid = str(r["account_id"])
            agg[aid]["atc"]           += float(r.get("atc") or 0)
            agg[aid]["atc_value"]     += float(r.get("atc_value") or 0)
            agg[aid]["checkout"]      += float(r.get("checkout") or 0)
            agg[aid]["purchase"]      += float(r.get("conversions") or 0)
            agg[aid]["purchase_value"]+= float(r.get("revenue") or 0)
        result = {}
        for aid, v in agg.items():
            result[aid] = {
                "atc":            int(v["atc"]),
                "atc_value":      round(v["atc_value"], 2),
                "checkout":       int(v["checkout"]),
                "purchase":       int(v["purchase"]),
                "purchase_value": round(v["purchase_value"], 2),
            }
        return result
    except Exception as e:
        print(f"[funnel] db read error: {e}")
        return {}


def _aggregate_adset_rows(rows: list) -> dict:
    """Aggregate adset_daily_metrics rows into per-adset totals + return latest sync time."""
    from collections import defaultdict
    agg: dict = defaultdict(lambda: {
        "adset_name": "", "spend": 0.0, "revenue": 0.0,
        "conversions": 0.0, "impressions": 0, "clicks": 0,
        "atc": 0.0, "checkout": 0.0, "ctr_sum": 0.0, "ctr_n": 0,
    })
    latest_sync = None
    for r in rows:
        aid = r["adset_id"]
        agg[aid]["adset_name"]  = r.get("adset_name") or aid
        agg[aid]["spend"]      += float(r.get("spend") or 0)
        agg[aid]["revenue"]    += float(r.get("revenue") or 0)
        agg[aid]["conversions"]+= float(r.get("conversions") or 0)
        agg[aid]["impressions"]+= int(r.get("impressions") or 0)
        agg[aid]["clicks"]     += int(r.get("clicks") or 0)
        agg[aid]["atc"]        += float(r.get("atc") or 0)
        agg[aid]["checkout"]   += float(r.get("checkout") or 0)
        ctr = float(r.get("ctr") or 0)
        if ctr > 0:
            agg[aid]["ctr_sum"] += ctr
            agg[aid]["ctr_n"]   += 1
        
        # Track latest sync time
        sync_time = r.get("synced_at")
        if sync_time:
            if latest_sync is None or sync_time > latest_sync:
                latest_sync = sync_time

    adsets = []
    for aid, m in agg.items():
        sp   = round(m["spend"], 2)
        rev  = round(m["revenue"], 2)
        clk  = m["clicks"]
        conv = round(m["conversions"], 1)
        adsets.append({
            "adset_id":    aid,
            "adset_name":  m["adset_name"],
            "spend":       sp,
            "revenue":     rev,
            "roas":        round(rev / sp, 2) if sp > 0 else 0.0,
            "conversions": conv,
            "impressions": m["impressions"],
            "clicks":      clk,
            "ctr":         round(m["ctr_sum"] / m["ctr_n"], 2) if m["ctr_n"] > 0 else 0.0,
            "atc":         int(m["atc"]),
            "checkout":    int(m["checkout"]),
            "cvr":         round(conv / clk * 100, 2) if clk > 0 else 0.0,
            "cpa":         round(sp / conv, 2) if conv > 0 else None,
        })
    adsets.sort(key=lambda x: x["spend"], reverse=True)
    return {"data": adsets, "last_synced": latest_sync}


def _aggregate_ad_rows(rows: list) -> dict:
    """Aggregate ad_daily_metrics rows into per-ad totals + return latest sync time."""
    from collections import defaultdict
    agg: dict = defaultdict(lambda: {
        "ad_name": "", "spend": 0.0, "revenue": 0.0,
        "conversions": 0.0, "impressions": 0, "clicks": 0,
        "atc": 0.0, "checkout": 0.0, "ctr_sum": 0.0, "ctr_n": 0,
        # Creative fields — static per ad, last non-empty value wins
        "ad_title": "", "ad_body": "", "creative_type": "",
        "thumbnail_url": "", "image_url": "",
        "call_to_action": "", "destination_url": "",
        # Status — most recent known value wins
        "ad_status": "UNKNOWN",
    })
    latest_sync = None
    for r in rows:
        ad_id = r["ad_id"]
        agg[ad_id]["ad_name"]     = r.get("ad_name") or ad_id
        agg[ad_id]["spend"]      += float(r.get("spend") or 0)
        agg[ad_id]["revenue"]    += float(r.get("revenue") or 0)
        agg[ad_id]["conversions"]+= float(r.get("conversions") or 0)
        agg[ad_id]["impressions"]+= int(r.get("impressions") or 0)
        agg[ad_id]["clicks"]     += int(r.get("clicks") or 0)
        agg[ad_id]["atc"]        += float(r.get("atc") or 0)
        agg[ad_id]["checkout"]   += float(r.get("checkout") or 0)
        ctr = float(r.get("ctr") or 0)
        if ctr > 0:
            agg[ad_id]["ctr_sum"] += ctr
            agg[ad_id]["ctr_n"]   += 1
        # Keep latest non-empty creative values
        for field in ("ad_title", "ad_body", "creative_type", "thumbnail_url",
                      "image_url", "call_to_action", "destination_url"):
            val = r.get(field, "")
            if val:
                agg[ad_id][field] = val
        # Keep most recent known status
        status = r.get("ad_status", "UNKNOWN") or "UNKNOWN"
        if status != "UNKNOWN":
            agg[ad_id]["ad_status"] = status
        
        # Track latest sync time
        sync_time = r.get("synced_at")
        if sync_time:
            if latest_sync is None or sync_time > latest_sync:
                latest_sync = sync_time

    ads = []
    for ad_id, m in agg.items():
        sp   = round(m["spend"], 2)
        rev  = round(m["revenue"], 2)
        clk  = m["clicks"]
        conv = round(m["conversions"], 1)
        ads.append({
            "ad_id":           ad_id,
            "ad_name":         m["ad_name"],
            "ad_status":       m["ad_status"],
            "spend":           sp,
            "revenue":         rev,
            "roas":            round(rev / sp, 2) if sp > 0 else 0.0,
            "conversions":     conv,
            "impressions":     m["impressions"],
            "clicks":          clk,
            "ctr":             round(m["ctr_sum"] / m["ctr_n"], 2) if m["ctr_n"] > 0 else 0.0,
            "atc":             int(m["atc"]),
            "checkout":        int(m["checkout"]),
            "cvr":             round(conv / clk * 100, 2) if clk > 0 else 0.0,
            "cpa":             round(sp / conv, 2) if conv > 0 else None,
            "ad_title":        m["ad_title"],
            "ad_body":         m["ad_body"],
            "creative_type":   m["creative_type"],
            "thumbnail_url":   m["thumbnail_url"],
            "image_url":       m["image_url"],
            "call_to_action":  m["call_to_action"],
            "destination_url": m["destination_url"],
        })
    ads.sort(key=lambda x: x["spend"], reverse=True)
    return {"data": ads, "last_synced": latest_sync}


@router.get("/{brand_id}/campaigns/{campaign_id}/adsets")
def get_campaign_adsets(
    brand_id: str,
    campaign_id: str,
    date_from: Optional[str] = Query(default=None),
    date_to: Optional[str] = Query(default=None),
    background_tasks: BackgroundTasks = None,
) -> Any:
    """
    Ad set metrics for a campaign.
    Reads from adset_daily_metrics (Supabase). If no data exists for the range,
    triggers a background sync from Meta and returns empty (client should retry).
    """
    from app.api.api_v1.endpoints.analytics import _default_dates
    if not date_from or not date_to:
        date_from, date_to = _default_dates()

    # 1. Read from DB
    try:
        resp = (
            supabase.table("adset_daily_metrics")
            .select("adset_id, adset_name, spend, revenue, roas, conversions, impressions, clicks, ctr, atc, checkout, synced_at")
            .eq("campaign_id", campaign_id)
            .gte("date", date_from)
            .lte("date", date_to)
            .execute()
        )
        rows = resp.data or []
    except Exception:
        rows = []

    # Check for staleness (6 hours)
    is_stale = False
    agg_result = {"data": [], "last_synced": None}
    if rows:
        agg_result = _aggregate_adset_rows(rows)
        if agg_result["last_synced"]:
            last_sync = datetime.fromisoformat(agg_result["last_synced"].replace("Z", "+00:00"))
            if datetime.now(last_sync.tzinfo) - last_sync > timedelta(hours=6):
                is_stale = True

    # Return whatever we have (or empty) while sync runs in background
    return {"data": agg_result["data"], "last_synced": agg_result["last_synced"]}


@router.post("/{brand_id}/campaigns/{campaign_id}/adsets/sync")
def sync_campaign_adsets(
    brand_id: str,
    campaign_id: str,
    date_from: Optional[str] = Query(default=None),
    date_to: Optional[str] = Query(default=None),
    background_tasks: BackgroundTasks = None,
) -> Any:
    """Trigger a background sync of ad set metrics for a campaign into Supabase."""
    from app.api.api_v1.endpoints.analytics import _default_dates
    from app.services.ingest import IngestService
    if not date_from or not date_to:
        date_from, date_to = _default_dates()
    try:
        camp_resp = (
            supabase.table("campaign_daily_metrics")
            .select("account_id")
            .eq("campaign_id", campaign_id)
            .limit(1)
            .execute()
        )
        account_id = camp_resp.data[0]["account_id"] if camp_resp.data else ""
    except Exception:
        account_id = ""
    if not account_id:
        raise HTTPException(status_code=404, detail="Campaign not found in DB")
    if background_tasks is not None:
        background_tasks.add_task(
            IngestService.sync_adset_daily_metrics,
            campaign_id, account_id, date_from, date_to, False,
        )
    return {"status": "sync_started", "campaign_id": campaign_id, "date_from": date_from, "date_to": date_to}


@router.get("/{brand_id}/adsets/{adset_id}/ads")
def get_adset_ads(
    brand_id: str,
    adset_id: str,
    date_from: Optional[str] = Query(default=None),
    date_to: Optional[str] = Query(default=None),
    background_tasks: BackgroundTasks = None,
) -> Any:
    """
    Ad-level metrics for an ad set.
    Reads from ad_daily_metrics (Supabase). If no data exists for the range,
    triggers a background sync from Meta and returns empty (client should retry).
    """
    from app.api.api_v1.endpoints.analytics import _default_dates
    if not date_from or not date_to:
        date_from, date_to = _default_dates()

    # 1. Read from DB
    try:
        resp = (
            supabase.table("ad_daily_metrics")
            .select("ad_id, ad_name, ad_status, spend, revenue, roas, conversions, impressions, clicks, ctr, atc, checkout, ad_title, ad_body, creative_type, thumbnail_url, image_url, call_to_action, destination_url, synced_at")
            .eq("adset_id", adset_id)
            .gte("date", date_from)
            .lte("date", date_to)
            .execute()
        )
        rows = resp.data or []
    except Exception:
        rows = []

    # Check if rows exist but are missing creative data (first sync stored metrics only)
    missing_creatives = rows and all(not r.get("ad_title") and not r.get("thumbnail_url") for r in rows)
    
    # Check for staleness (6 hours)
    is_stale = False
    agg_result = {"data": [], "last_synced": None}
    if rows:
        agg_result = _aggregate_ad_rows(rows)
        if agg_result["last_synced"]:
            last_sync = datetime.fromisoformat(agg_result["last_synced"].replace("Z", "+00:00"))
            if datetime.now(last_sync.tzinfo) - last_sync > timedelta(hours=6):
                is_stale = True

    # No rows OR rows have no creatives OR stale — find parent IDs
    try:
        adset_resp = (
            supabase.table("adset_daily_metrics")
            .select("campaign_id, account_id")
            .eq("adset_id", adset_id)
            .limit(1)
            .execute()
        )
        parent = adset_resp.data[0] if adset_resp.data else {}
        campaign_id = parent.get("campaign_id", "")
        account_id  = parent.get("account_id", "")
    except Exception:
        campaign_id = ""
        account_id  = ""

    if account_id and background_tasks is not None:
        from app.services.ingest import IngestService
        if missing_creatives and not is_stale:
            # Fast path: rows exist but lack creatives — update only creative fields
            background_tasks.add_task(IngestService.backfill_ad_creatives, adset_id)
        else:
            # No rows at all OR stale — need a full metrics + creative sync
            background_tasks.add_task(
                IngestService.sync_ad_daily_metrics,
                adset_id, campaign_id, account_id, date_from, date_to, False if is_stale else True,
            )

    # Return whatever we have (metrics without creatives) while sync runs in background
    return agg_result["data"]


@router.post("/{brand_id}/adsets/{adset_id}/ads/sync")
def sync_adset_ads(
    brand_id: str,
    adset_id: str,
    date_from: Optional[str] = Query(default=None),
    date_to: Optional[str] = Query(default=None),
    background_tasks: BackgroundTasks = None,
) -> Any:
    """Trigger a background sync of ad metrics for an ad set into Supabase."""
    from app.api.api_v1.endpoints.analytics import _default_dates
    from app.services.ingest import IngestService
    if not date_from or not date_to:
        date_from, date_to = _default_dates()
    try:
        adset_resp = (
            supabase.table("adset_daily_metrics")
            .select("campaign_id, account_id")
            .eq("adset_id", adset_id)
            .limit(1)
            .execute()
        )
        parent = adset_resp.data[0] if adset_resp.data else {}
        campaign_id = parent.get("campaign_id", "")
        account_id  = parent.get("account_id", "")
    except Exception:
        campaign_id = ""
        account_id  = ""
    if not account_id:
        raise HTTPException(status_code=404, detail="Ad set not found in DB")
    if background_tasks is not None:
        background_tasks.add_task(
            IngestService.sync_ad_daily_metrics,
            adset_id, campaign_id, account_id, date_from, date_to, False,
        )
    return {"status": "sync_started", "adset_id": adset_id, "date_from": date_from, "date_to": date_to}


@router.get("/{brand_id}/summary")
def get_brand_summary(brand_id: str) -> Any:
    """
    Get performance summary filtered by brand.
    Pulls account IDs from brand_accounts, then fetches their metrics.
    """
    try:
        # Get accounts for this brand
        accts_resp = supabase.table("brand_accounts").select("*").eq("brand_id", brand_id).execute()
        accounts = accts_resp.data or []

        if not accounts:
            return {"error": "No accounts mapped to this brand", "brand_id": brand_id}

        account_ids = [a["account_id"] for a in accounts]

        # Get metrics from performance_metrics table
        metrics_resp = supabase.table("performance_metrics").select("*").in_("entity_id", account_ids).execute()
        metrics = metrics_resp.data or []

        total_spend = sum(float(m.get("spend") or 0) for m in metrics)
        total_revenue = sum(float(m.get("revenue") or 0) for m in metrics)
        roas = total_revenue / total_spend if total_spend > 0 else 0

        return {
            "brand_id": brand_id,
            "accounts_count": len(accounts),
            "metrics": {
                "total_spend": round(total_spend, 2),
                "total_revenue": round(total_revenue, 2),
                "roas": round(roas, 2),
            },
            "accounts": accounts,
            "entities": metrics
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
