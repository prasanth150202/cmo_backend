from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from app.core.config import settings
from app.schemas.metrics import EntityContext, AdMetrics
from typing import List, Dict, Any, Optional


def _extract_action(data: Optional[List[Dict]], action_type: str) -> float:
    """Pull a single value from Meta's actions/action_values array."""
    for item in (data or []):
        if item.get("action_type") == action_type:
            return float(item.get("value", 0))
    return 0.0


class MetaService:
    def __init__(self):
        if settings.META_SYSTEM_USER_TOKEN:
            FacebookAdsApi.init(access_token=settings.META_SYSTEM_USER_TOKEN, api_version='v22.0')

    def get_account_metrics(self, ad_account_id: str) -> List[Dict[str, Any]]:
        """
        Pull campaign-level insights from Meta including spend, revenue, ROAS, conversions.
        Uses the Insights API at campaign level so action_values and purchase_roas are available.
        """
        if not ad_account_id.startswith("act_"):
            ad_account_id = f"act_{ad_account_id}"

        account = AdAccount(ad_account_id)

        insights = account.get_insights(
            fields=[
                "campaign_id",
                "campaign_name",
                "spend",
                "impressions",
                "clicks",
                "ctr",
                "actions",
                "action_values",
                "purchase_roas",
            ],
            params={
                "level": "campaign",
                "date_preset": "last_7d",
                "limit": 200,
            },
        )

        result = []
        for row in insights:
            data = row.export_all_data()
            data["account_id"] = ad_account_id
            result.append(data)

        return result

    def normalize_to_ctx(self, meta_data: Dict[str, Any]) -> EntityContext:
        """Convert a Meta Insights row into our Rule Engine's EntityContext."""
        spend = float(meta_data.get("spend", 0) or 0)

        # Revenue: use omni_purchase from action_values (covers on-site + off-site)
        revenue = _extract_action(meta_data.get("action_values"), "omni_purchase")

        # Conversions: purchase count from actions
        conversions = _extract_action(meta_data.get("actions"), "omni_purchase")

        # ROAS: from Meta's purchase_roas field, else calculate
        roas_list = meta_data.get("purchase_roas")
        if roas_list:
            roas = _extract_action(roas_list, "omni_purchase")
        else:
            roas = round(revenue / spend, 2) if spend > 0 else 0.0

        impressions = int(meta_data.get("impressions", 0) or 0)
        clicks = int(meta_data.get("clicks", 0) or 0)
        ctr = float(meta_data.get("ctr", 0) or 0)

        m7d = AdMetrics(
            spend=round(spend, 2),
            revenue=round(revenue, 2),
            roas=round(roas, 2),
            conversions=round(conversions, 1),
            impressions=impressions,
            clicks=clicks,
            ctr=round(ctr, 2),
        )

        # Derive shorter windows from Supabase campaign_daily_metrics to avoid extra API calls
        campaign_id = meta_data.get("campaign_id", meta_data.get("id", ""))
        account_id  = meta_data.get("account_id", "")
        m1d = m3d = m14d = m30d = m7d
        if campaign_id:
            try:
                from app.db.supabase import supabase as _sb
                from datetime import datetime, timedelta
                today_str = datetime.now().strftime("%Y-%m-%d")

                def _window(days: int) -> AdMetrics:
                    since = (datetime.now() - timedelta(days=days - 1)).strftime("%Y-%m-%d")
                    resp  = (
                        _sb.table("campaign_daily_metrics")
                        .select("spend, revenue, conversions, impressions, clicks, ctr")
                        .eq("campaign_id", campaign_id)
                        .gte("date", since)
                        .lte("date", today_str)
                        .execute()
                    )
                    rows = resp.data or []
                    if not rows:
                        return m7d
                    sp  = sum(float(r.get("spend") or 0) for r in rows)
                    rev = sum(float(r.get("revenue") or 0) for r in rows)
                    conv = sum(float(r.get("conversions") or 0) for r in rows)
                    ctrs = [float(r.get("ctr") or 0) for r in rows if r.get("ctr")]
                    return AdMetrics(
                        spend=round(sp, 2),
                        revenue=round(rev, 2),
                        roas=round(rev / sp, 2) if sp > 0 else 0.0,
                        conversions=round(conv, 1),
                        impressions=sum(int(r.get("impressions") or 0) for r in rows),
                        clicks=sum(int(r.get("clicks") or 0) for r in rows),
                        ctr=round(sum(ctrs) / len(ctrs), 2) if ctrs else 0.0,
                    )

                m1d  = _window(1)
                m3d  = _window(3)
                m14d = _window(14)
                m30d = _window(30)
            except Exception:
                pass

        return EntityContext(
            entity_id=campaign_id,
            entity_name=meta_data.get("campaign_name", meta_data.get("name", "")),
            account_id=account_id,
            m7d=m7d,
            m1d=m1d,
            m3d=m3d,
            m14d=m14d,
            m30d=m30d,
            today=m1d,
            current_budget=0.0,
        )


# Global instance
meta_service = MetaService()
