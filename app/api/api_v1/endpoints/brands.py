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

    try:
        brand_resp = supabase.table("brands").select("*").eq("id", brand_id).execute()
        if not brand_resp.data:
            raise HTTPException(status_code=404, detail="Brand not found")
        brand = brand_resp.data[0]

        accts_resp = supabase.table("brand_accounts").select("*").eq("brand_id", brand_id).execute()
        accounts = accts_resp.data or []
        account_ids = {a["account_id"].replace("act_", "") for a in accounts}

        if not account_ids:
            return {"brand": brand, "accounts": [], "today": {}, "daily": [], "funnel": {}}

        # ── Daily time-series from local store ─────────────────────────────────
        daily_resp = (
            supabase.table("daily_metrics")
            .select("*")
            .in_("account_id", list(account_ids))
            .gte("date", date_from)
            .lte("date", date_to)
            .order("date")
            .execute()
        )
        daily_rows = daily_resp.data or []

        # Aggregate by date
        by_date: dict = defaultdict(lambda: {"spend": 0.0, "revenue": 0.0, "impressions": 0, "clicks": 0, "ctr_sum": 0.0, "ctr_n": 0, "conversions": 0.0})
        for r in daily_rows:
            d = str(r["date"])[:10]
            by_date[d]["spend"]       += float(r.get("spend") or 0)
            by_date[d]["revenue"]     += float(r.get("revenue") or 0)
            by_date[d]["impressions"] += int(r.get("impressions") or 0)
            by_date[d]["clicks"]      += int(r.get("clicks") or 0)
            by_date[d]["conversions"] += float(r.get("conversions") or 0)
            ctr = float(r.get("ctr") or 0)
            if ctr > 0:
                by_date[d]["ctr_sum"] += ctr
                by_date[d]["ctr_n"]   += 1

        daily_agg = []
        for d in sorted(by_date.keys()):
            row = by_date[d]
            sp  = round(row["spend"], 2)
            rev = round(row["revenue"], 2)
            daily_agg.append({
                "date":        d,
                "spend":       sp,
                "revenue":     rev,
                "roas":        round(rev / sp, 2) if sp > 0 else 0.0,
                "impressions": row["impressions"],
                "clicks":      row["clicks"],
                "conversions": round(row["conversions"], 1),
                "ctr":         round(row["ctr_sum"] / row["ctr_n"], 2) if row["ctr_n"] > 0 else 0.0,
            })

        # ── Aggregate over the full selected date range ────────────────────────
        total_spend       = round(sum(float(r.get("spend") or 0) for r in daily_rows), 2)
        total_revenue     = round(sum(float(r.get("revenue") or 0) for r in daily_rows), 2)
        total_roas        = round(total_revenue / total_spend, 2) if total_spend > 0 else 0.0
        total_impressions = sum(int(r.get("impressions") or 0) for r in daily_rows)
        total_clicks      = sum(int(r.get("clicks") or 0) for r in daily_rows)
        total_conversions = round(sum(float(r.get("conversions") or 0) for r in daily_rows), 1)
        ctr_vals          = [float(r.get("ctr") or 0) for r in daily_rows if r.get("ctr")]
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
            target_roas = float(brand.get("target_roas") or 3.0)
            score = min(100, int((roas / target_roas) * 60 + min(cvr * 10, 40))) if roas > 0 else 0
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
        camp_resp = (
            supabase.table("campaign_daily_metrics")
            .select("campaign_id, campaign_name, account_id, spend, revenue, roas, conversions, impressions, clicks, ctr, atc, checkout")
            .in_("account_id", list(account_ids))
            .gte("date", date_from)
            .lte("date", date_to)
            .execute()
        )
        camp_rows = camp_resp.data or []
        camp_agg: dict = defaultdict(lambda: {
            "campaign_name": "", "account_id": "",
            "spend": 0.0, "revenue": 0.0, "conversions": 0.0,
            "impressions": 0, "clicks": 0, "atc": 0.0, "checkout": 0.0,
            "ctr_sum": 0.0, "ctr_n": 0,
        })
        for r in camp_rows:
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
            ctr = float(r.get("ctr") or 0)
            if ctr > 0:
                camp_agg[cid]["ctr_sum"] += ctr
                camp_agg[cid]["ctr_n"]   += 1

        campaign_ids = list(camp_agg.keys())
        status_map = {}
        if campaign_ids:
            s_resp = supabase.table("campaigns").select("id, status").in_("id", campaign_ids).execute()
            status_map = {c["id"]: c.get("status", "ACTIVE") for c in s_resp.data or []}

        campaigns = []
        for cid, m in camp_agg.items():
            sp  = round(m["spend"], 2)
            rev = round(m["revenue"], 2)
            clk = m["clicks"]
            conv = round(m["conversions"], 1)
            campaigns.append({
                "campaign_id":   cid,
                "status":        status_map.get(cid, "ACTIVE"),
                "campaign_name": m["campaign_name"],
                "account_id":    m["account_id"],
                "spend":         sp,
                "revenue":       rev,
                "roas":          round(rev / sp, 2) if sp > 0 else 0.0,
                "conversions":   conv,
                "impressions":   m["impressions"],
                "clicks":        clk,
                "ctr":           round(m["ctr_sum"] / m["ctr_n"], 2) if m["ctr_n"] > 0 else 0.0,
                "atc":           int(m["atc"]),
                "checkout":      int(m["checkout"]),
                "cvr":           round(conv / clk * 100, 2) if clk > 0 else 0.0,
                "cpa":           round(sp / conv, 2) if conv > 0 else None,
            })
        campaigns.sort(key=lambda x: x["spend"], reverse=True)

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


def _aggregate_adset_rows(rows: list) -> list:
    """Aggregate adset_daily_metrics rows (multiple dates) into per-adset totals."""
    from collections import defaultdict
    agg: dict = defaultdict(lambda: {
        "adset_name": "", "spend": 0.0, "revenue": 0.0,
        "conversions": 0.0, "impressions": 0, "clicks": 0,
        "atc": 0.0, "checkout": 0.0, "ctr_sum": 0.0, "ctr_n": 0,
    })
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
    result = []
    for aid, m in agg.items():
        sp   = round(m["spend"], 2)
        rev  = round(m["revenue"], 2)
        clk  = m["clicks"]
        conv = round(m["conversions"], 1)
        result.append({
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
    result.sort(key=lambda x: x["spend"], reverse=True)
    return result


def _aggregate_ad_rows(rows: list) -> list:
    """Aggregate ad_daily_metrics rows into per-ad totals, preserving creative fields."""
    from collections import defaultdict
    agg: dict = defaultdict(lambda: {
        "ad_name": "", "spend": 0.0, "revenue": 0.0,
        "conversions": 0.0, "impressions": 0, "clicks": 0,
        "atc": 0.0, "checkout": 0.0, "ctr_sum": 0.0, "ctr_n": 0,
        # Creative fields — static per ad, last value wins (they don't change)
        "ad_title": "", "ad_body": "", "creative_type": "",
        "thumbnail_url": "", "image_url": "",
        "call_to_action": "", "destination_url": "",
    })
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
    result = []
    for ad_id, m in agg.items():
        sp   = round(m["spend"], 2)
        rev  = round(m["revenue"], 2)
        clk  = m["clicks"]
        conv = round(m["conversions"], 1)
        result.append({
            "ad_id":           ad_id,
            "ad_name":         m["ad_name"],
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
    result.sort(key=lambda x: x["spend"], reverse=True)
    return result


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
            .select("adset_id, adset_name, spend, revenue, roas, conversions, impressions, clicks, ctr, atc, checkout")
            .eq("campaign_id", campaign_id)
            .gte("date", date_from)
            .lte("date", date_to)
            .execute()
        )
        rows = resp.data or []
    except Exception:
        rows = []

    if rows:
        return _aggregate_adset_rows(rows)

    # 2. No DB data — find account_id for this campaign then trigger background sync
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

    if account_id and background_tasks is not None:
        from app.services.ingest import IngestService
        background_tasks.add_task(
            IngestService.sync_adset_daily_metrics,
            campaign_id, account_id, date_from, date_to,
        )

    return []


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
            .select("ad_id, ad_name, spend, revenue, roas, conversions, impressions, clicks, ctr, atc, checkout, ad_title, ad_body, creative_type, thumbnail_url, image_url, call_to_action, destination_url")
            .eq("adset_id", adset_id)
            .gte("date", date_from)
            .lte("date", date_to)
            .execute()
        )
        rows = resp.data or []
    except Exception:
        rows = []

    if rows:
        return _aggregate_ad_rows(rows)

    # 2. No DB data — find campaign_id + account_id then trigger background sync
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
        background_tasks.add_task(
            IngestService.sync_ad_daily_metrics,
            adset_id, campaign_id, account_id, date_from, date_to,
        )

    return []


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
