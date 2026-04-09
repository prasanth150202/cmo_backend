from typing import Any, Dict, List, Optional
from fastapi import APIRouter, HTTPException, Query
from datetime import datetime, timedelta
from collections import defaultdict
from app.db.supabase import supabase

router = APIRouter()


def _extract_action(data: Optional[List[Dict]], action_type: str) -> float:
    for item in (data or []):
        if item.get("action_type") == action_type:
            return float(item.get("value", 0))
    return 0.0


def _default_dates():
    today = datetime.now().strftime("%Y-%m-%d")
    since = (datetime.now() - timedelta(days=29)).strftime("%Y-%m-%d")
    return since, today


def _read_daily_metrics(date_from: str, date_to: str, brand_id: Optional[str] = None) -> List[Dict]:
    """
    Read from the local daily_metrics table in Supabase.
    Returns one row per (date, account_id).
    Optionally filters by brand_id.
    """
    try:
        query = supabase.table("daily_metrics").select("*")

        if brand_id:
            # Fetch account IDs associated with the brand
            brand_accounts_resp = supabase.table("brand_accounts").select("account_id").eq("brand_id", brand_id).execute()
            account_ids = [a["account_id"].replace("act_", "") for a in brand_accounts_resp.data or []]
            if not account_ids:
                return [] # No accounts for this brand, so no daily metrics

            query = query.in_("account_id", account_ids)

        resp = (
            query
            .gte("date", date_from)
            .lte("date", date_to)
            .order("date")
            .execute()
        )
        return resp.data or []
    except Exception as e:
        print(f"daily_metrics read error: {e}")
        return []


def _aggregate_daily(rows: List[Dict]) -> List[Dict]:
    """
    Aggregate per-(date, account_id) rows into per-date totals
    so analytics charts show one number per day across all accounts.
    """
    by_date: Dict[str, Dict] = defaultdict(lambda: {
        "spend": 0.0, "revenue": 0.0, "conversions": 0.0,
        "impressions": 0, "clicks": 0, "ctr_sum": 0.0, "ctr_n": 0,
    })
    for r in rows:
        date = str(r["date"])[:10]
        by_date[date]["spend"] += float(r.get("spend") or 0)
        by_date[date]["revenue"] += float(r.get("revenue") or 0)
        by_date[date]["conversions"] += float(r.get("conversions") or 0)
        by_date[date]["impressions"] += int(r.get("impressions") or 0)
        by_date[date]["clicks"] += int(r.get("clicks") or 0)
        ctr = float(r.get("ctr") or 0)
        if ctr > 0:
            by_date[date]["ctr_sum"] += ctr
            by_date[date]["ctr_n"] += 1

    result = []
    for date in sorted(by_date.keys()):
        row = by_date[date]
        spend = round(row["spend"], 2)
        revenue = round(row["revenue"], 2)
        roas = round(revenue / spend, 2) if spend > 0 else 0.0
        ctr = round(row["ctr_sum"] / row["ctr_n"], 2) if row["ctr_n"] > 0 else 0.0
        result.append({
            "date": date,
            "spend": spend,
            "revenue": revenue,
            "roas": roas,
            "conversions": round(row["conversions"], 1),
            "impressions": row["impressions"],
            "clicks": row["clicks"],
            "ctr": ctr,
        })
    return result


def _fetch_daily_from_meta(date_from: str, date_to: str, brand_id: Optional[str] = None) -> List[Dict]:
    """
    Direct Meta Insights pull. Used as fallback when daily_metrics has no data.
    After fetching, stores results into daily_metrics for future reads.
    Optionally filters by brand_id.
    """
    from facebook_business.api import FacebookAdsApi
    from facebook_business.adobjects.adaccount import AdAccount
    from app.core.config import settings

    if not settings.META_SYSTEM_USER_TOKEN:
        return []

    FacebookAdsApi.init(access_token=settings.META_SYSTEM_USER_TOKEN)

    try:
        query = supabase.table("brand_accounts").select("account_id, platform")
        if brand_id:
            query = query.eq("brand_id", brand_id)
        accts_resp = query.execute()

        meta_accounts = [
            a["account_id"] for a in (accts_resp.data or [])
            if a.get("platform", "").upper() == "META"
        ]
    except Exception:
        meta_accounts = []

    if not meta_accounts:
        return []

    daily: Dict[str, Dict] = defaultdict(lambda: {
        "spend": 0.0, "revenue": 0.0, "conversions": 0.0,
        "impressions": 0, "clicks": 0, "ctr_sum": 0.0, "ctr_n": 0,
    })

    for account_id in meta_accounts:
        norm_id = account_id if account_id.startswith("act_") else f"act_{account_id}"
        clean_id = account_id.replace("act_", "")
        try:
            account = AdAccount(norm_id)
            insights = account.get_insights(
                fields=["spend", "impressions", "clicks", "ctr", "actions", "action_values"],
                params={
                    "level": "account",
                    "time_range": {"since": date_from, "until": date_to},
                    "time_increment": 1,
                    "limit": 500,
                },
            )
            for row in insights:
                d = row.export_all_data()
                date = d.get("date_start", "")
                if not date:
                    continue
                spend = float(d.get("spend", 0) or 0)
                revenue = _extract_action(d.get("action_values"), "omni_purchase")
                conversions = _extract_action(d.get("actions"), "omni_purchase")
                impressions = int(d.get("impressions", 0) or 0)
                clicks = int(d.get("clicks", 0) or 0)
                ctr = float(d.get("ctr", 0) or 0)
                roas = round(revenue / spend, 2) if spend > 0 else 0.0

                atc       = _extract_action(d.get("actions"),     "add_to_cart")
                atc_value = _extract_action(d.get("action_values"), "add_to_cart")
                checkout  = _extract_action(d.get("actions"),     "initiate_checkout")

                # Store into daily_metrics so future reads are local
                try:
                    supabase.table("daily_metrics").upsert(
                        {
                            "date":        date,
                            "account_id":  clean_id,
                            "spend":       round(spend, 2),
                            "revenue":     round(revenue, 2),
                            "roas":        roas,
                            "conversions": round(conversions, 1),
                            "impressions": impressions,
                            "clicks":      clicks,
                            "ctr":         round(ctr, 2),
                            "atc":         round(atc, 1),
                            "atc_value":   round(atc_value, 2),
                            "checkout":    round(checkout, 1),
                            "synced_at":   datetime.utcnow().isoformat(),
                        },
                        on_conflict="date,account_id",
                    ).execute()
                except Exception as e:
                    print(f"daily_metrics upsert error {date}/{clean_id}: {e}")

                daily[date]["spend"] += spend
                daily[date]["revenue"] += revenue
                daily[date]["conversions"] += conversions
                daily[date]["impressions"] += impressions
                daily[date]["clicks"] += clicks
                if ctr > 0:
                    daily[date]["ctr_sum"] += ctr
                    daily[date]["ctr_n"] += 1
        except Exception as e:
            print(f"Meta insights error for {account_id}: {e}")

    result = []
    for date in sorted(daily.keys()):
        row = daily[date]
        spend = round(row["spend"], 2)
        revenue = round(row["revenue"], 2)
        roas = round(revenue / spend, 2) if spend > 0 else 0.0
        ctr = round(row["ctr_sum"] / row["ctr_n"], 2) if row["ctr_n"] > 0 else 0.0
        result.append({
            "date": date,
            "spend": spend,
            "revenue": revenue,
            "roas": roas,
            "conversions": round(row["conversions"], 1),
            "impressions": row["impressions"],
            "clicks": row["clicks"],
            "ctr": ctr,
        })
    return result


def _fetch_account_totals(date_from: str, date_to: str) -> dict:
    """
    Returns spend/revenue/conversions totals keyed by account_id.
    Reads from local daily_metrics first; falls back to Meta API.
    """
    rows = _read_daily_metrics(date_from, date_to)

    if rows:
        totals: dict = defaultdict(lambda: {"spend": 0.0, "revenue": 0.0, "conversions": 0.0})
        for r in rows:
            aid = str(r["account_id"])
            totals[aid]["spend"] += float(r.get("spend") or 0)
            totals[aid]["revenue"] += float(r.get("revenue") or 0)
            totals[aid]["conversions"] += float(r.get("conversions") or 0)
        result = {}
        for aid, t in totals.items():
            result[aid] = {
                "spend": round(t["spend"], 2),
                "revenue": round(t["revenue"], 2),
                "conversions": round(t["conversions"], 1),
                "roas": round(t["revenue"] / t["spend"], 2) if t["spend"] > 0 else 0.0,
            }
        return result

    # Fallback: hit Meta API directly
    from facebook_business.api import FacebookAdsApi
    from facebook_business.adobjects.adaccount import AdAccount
    from app.core.config import settings

    if not settings.META_SYSTEM_USER_TOKEN:
        return {}

    FacebookAdsApi.init(access_token=settings.META_SYSTEM_USER_TOKEN)

    try:
        accts_resp = supabase.table("brand_accounts").select("account_id, platform").execute()
        meta_accounts = [
            a["account_id"] for a in (accts_resp.data or [])
            if a.get("platform", "").upper() == "META"
        ]
    except Exception:
        meta_accounts = []

    totals = {}
    for account_id in meta_accounts:
        norm_id = account_id if account_id.startswith("act_") else f"act_{account_id}"
        clean_id = account_id.replace("act_", "")
        try:
            account = AdAccount(norm_id)
            insights = account.get_insights(
                fields=["spend", "actions", "action_values"],
                params={
                    "level": "account",
                    "time_range": {"since": date_from, "until": date_to},
                    "limit": 500,
                },
            )
            spend, revenue, conversions = 0.0, 0.0, 0.0
            for row in insights:
                d = row.export_all_data()
                spend += float(d.get("spend", 0) or 0)
                revenue += _extract_action(d.get("action_values"), "omni_purchase")
                conversions += _extract_action(d.get("actions"), "omni_purchase")
            totals[clean_id] = {
                "spend": round(spend, 2),
                "revenue": round(revenue, 2),
                "conversions": round(conversions, 1),
                "roas": round(revenue / spend, 2) if spend > 0 else 0.0,
            }
        except Exception as e:
            print(f"Account totals error for {account_id}: {e}")

    return totals


@router.get("/overview")
def get_analytics_overview(
    date_from: Optional[str] = Query(default=None),
    date_to: Optional[str] = Query(default=None),
    brand_id: Optional[str] = Query(default=None),
) -> Any:
    """Daily performance breakdown. Reads from local daily_metrics; falls back to Meta API."""
    try:
        if not date_from or not date_to:
            date_from, date_to = _default_dates()

        # 1. Try local Supabase store
        rows = _read_daily_metrics(date_from, date_to, brand_id)
        if rows:
            return {"daily": _aggregate_daily(rows), "source": "local"}

        # 2. Fall back to Meta API (also writes to daily_metrics for next time)
        daily = _fetch_daily_from_meta(date_from, date_to, brand_id)
        if daily:
            return {"daily": daily, "source": "meta"}

        # 3. Last resort: performance_metrics snapshot
        resp = supabase.table("performance_metrics").select("*").order("created_at").execute()
        metrics = resp.data or []
        by_date: Dict[str, List] = defaultdict(list)
        for m in metrics:
            date = m.get("created_at", "")[:10]
            if date and date_from <= date <= date_to:
                by_date[date].append(m)
        daily = []
        for date in sorted(by_date.keys()):
            rows_pm = by_date[date]
            spend = round(sum(float(r.get("spend") or 0) for r in rows_pm), 2)
            revenue = round(sum(float(r.get("revenue") or 0) for r in rows_pm), 2)
            daily.append({
                "date": date,
                "spend": spend,
                "revenue": revenue,
                "roas": round(revenue / spend, 2) if spend > 0 else 0,
                "conversions": round(sum(float(r.get("conversions") or 0) for r in rows_pm), 1),
                "impressions": 0,
                "clicks": 0,
                "ctr": 0,
            })
        return {"daily": daily, "source": "snapshot"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/by-channel")
def get_channel_breakdown(
    date_from: Optional[str] = Query(default=None),
    date_to: Optional[str] = Query(default=None),
    brand_id: Optional[str] = Query(default=None),
) -> Any:
    """Channel breakdown. Reads from local daily_metrics."""
    try:
        if not date_from or not date_to:
            date_from, date_to = _default_dates()

        rows = _read_daily_metrics(date_from, date_to, brand_id)
        if rows:
            spend = round(sum(float(r.get("spend") or 0) for r in rows), 2)
            revenue = round(sum(float(r.get("revenue") or 0) for r in rows), 2)
            roas = round(revenue / spend, 2) if spend > 0 else 0.0
            conversions = round(sum(float(r.get("conversions") or 0) for r in rows), 0)
            return [{"channel": "META", "spend": spend, "revenue": revenue, "roas": roas, "conversions": conversions}]

        daily = _fetch_daily_from_meta(date_from, date_to, brand_id)
        if daily:
            spend = round(sum(r["spend"] for r in daily), 2)
            revenue = round(sum(r["revenue"] for r in daily), 2)
            roas = round(revenue / spend, 2) if spend > 0 else 0.0
            return [{"channel": "META", "spend": spend, "revenue": revenue, "roas": roas, "conversions": round(sum(r["conversions"] for r in daily), 0)}]

        resp = supabase.table("performance_metrics").select("*").execute()
        metrics = resp.data or []
        spend = round(sum(float(m.get("spend") or 0) for m in metrics), 2)
        revenue = round(sum(float(m.get("revenue") or 0) for m in metrics), 2)
        roas = round(revenue / spend, 2) if spend > 0 else 0.0
        return [{"channel": "META", "spend": spend, "revenue": revenue, "roas": roas, "conversions": 0}]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/campaigns")
def get_campaign_breakdown(
    date_from: Optional[str] = Query(default=None),
    date_to:   Optional[str] = Query(default=None),
    brand_id:  Optional[str] = Query(default=None),
) -> Any:
    """
    Campaign-level breakdown aggregated over the date range.
    Reads from campaign_daily_metrics — 100% from DB.
    Optionally filters by brand_id.
    """
    try:
        if not date_from or not date_to:
            date_from, date_to = _default_dates()

        query = supabase.table("campaign_daily_metrics").select("campaign_id, campaign_name, account_id, spend, revenue, roas, conversions, impressions, clicks, ctr, atc, checkout")

        if brand_id:
            # Fetch account IDs associated with the brand
            brand_accounts_resp = supabase.table("brand_accounts").select("account_id").eq("brand_id", brand_id).execute()
            account_ids = [a["account_id"].replace("act_", "") for a in brand_accounts_resp.data or []]
            if not account_ids:
                return [] # No accounts for this brand, so no campaign metrics

            query = query.in_("account_id", account_ids)

        resp = (
            query
            .gte("date", date_from)
            .lte("date", date_to)
            .execute()
        )
        rows = resp.data or []

        # Aggregate per campaign across all days
        from collections import defaultdict
        agg: dict = defaultdict(lambda: {
            "campaign_name": "", "account_id": "",
            "spend": 0.0, "revenue": 0.0, "conversions": 0.0,
            "impressions": 0, "clicks": 0, "atc": 0.0, "checkout": 0.0,
            "ctr_sum": 0.0, "ctr_n": 0,
        })
        for r in rows:
            cid = r["campaign_id"]
            agg[cid]["campaign_name"] = r.get("campaign_name") or cid
            agg[cid]["account_id"]    = r.get("account_id", "")
            agg[cid]["spend"]        += float(r.get("spend") or 0)
            agg[cid]["revenue"]      += float(r.get("revenue") or 0)
            agg[cid]["conversions"]  += float(r.get("conversions") or 0)
            agg[cid]["impressions"]  += int(r.get("impressions") or 0)
            agg[cid]["clicks"]       += int(r.get("clicks") or 0)
            agg[cid]["atc"]          += float(r.get("atc") or 0)
            agg[cid]["checkout"]     += float(r.get("checkout") or 0)
            ctr = float(r.get("ctr") or 0)
            if ctr > 0:
                agg[cid]["ctr_sum"] += ctr
                agg[cid]["ctr_n"]   += 1

        campaign_ids = list(agg.keys())
        status_map = {}
        if campaign_ids:
            # Batch fetch statuses (chunking safely handles up to 100-200 easily)
            s_resp = supabase.table("campaigns").select("id, status").in_("id", campaign_ids).execute()
            status_map = {c["id"]: c.get("status", "ACTIVE") for c in s_resp.data or []}

        result = []
        for cid, m in agg.items():
            sp  = round(m["spend"], 2)
            rev = round(m["revenue"], 2)
            clk = m["clicks"]
            conv = round(m["conversions"], 1)
            result.append({
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

        # Sort by spend descending
        result.sort(key=lambda x: x["spend"], reverse=True)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/entities")
def get_entities() -> Any:
    """All campaign rows from performance_metrics ordered by spend."""
    try:
        resp = supabase.table("performance_metrics").select("*").order("spend", desc=True).execute()
        return resp.data or []
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
