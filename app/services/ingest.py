import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from app.services.meta import meta_service, _extract_action
from app.db.supabase import supabase
from app.services.rules.executor import executor

# Account-level: 90-day chunks are safe
CHUNK_DAYS = 90
# Campaign-level: smaller chunks — more rows per request, more API cost
CAMPAIGN_CHUNK_DAYS = 30
# Seconds to wait between chunks
CHUNK_DELAY = 2.0
# Max retries on rate-limit / transient errors
MAX_RETRIES = 3


def _date_chunks(date_from: str, date_to: str, chunk_days: int = CHUNK_DAYS):
    """Yield (chunk_from, chunk_to) pairs covering date_from → date_to."""
    start = datetime.strptime(date_from, "%Y-%m-%d")
    end   = datetime.strptime(date_to,   "%Y-%m-%d")
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=chunk_days - 1), end)
        yield cursor.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")
        cursor = chunk_end + timedelta(days=1)


def _covered_dates(account_id: str, date_from: str, date_to: str) -> set:
    """Return dates already stored in daily_metrics for this account/range."""
    try:
        resp = (
            supabase.table("daily_metrics")
            .select("date")
            .eq("account_id", account_id)
            .gte("date", date_from)
            .lte("date", date_to)
            .execute()
        )
        return {str(r["date"])[:10] for r in (resp.data or [])}
    except Exception:
        return set()


def _update_job(job_id: str, **fields) -> None:
    try:
        supabase.table("sync_jobs").update(
            {**fields, "updated_at": datetime.utcnow().isoformat()}
        ).eq("id", job_id).execute()
    except Exception as e:
        print(f"[ingest] job update error: {e}")


def _pull_chunk(account_id: str, date_from: str, date_to: str) -> int:
    """
    Pull one 90-day chunk from Meta for a single account.
    Returns number of rows upserted. Retries on rate-limit errors.
    """
    from facebook_business.api import FacebookAdsApi
    from facebook_business.adobjects.adaccount import AdAccount
    from app.core.config import settings

    FacebookAdsApi.init(access_token=settings.META_SYSTEM_USER_TOKEN, api_version='v22.0')
    norm_id  = account_id if account_id.startswith("act_") else f"act_{account_id}"
    clean_id = account_id.replace("act_", "")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            insights = AdAccount(norm_id).get_insights(
                fields=["spend", "impressions", "clicks", "ctr", "actions", "action_values"],
                params={
                    "level": "account",
                    "time_range": {"since": date_from, "until": date_to},
                    "time_increment": 1,
                    "limit": 500,
                },
            )
            rows_upserted = 0
            for row in insights:
                d = row.export_all_data()
                date = d.get("date_start", "")
                if not date:
                    continue
                spend       = float(d.get("spend", 0) or 0)
                actions     = d.get("actions")
                action_vals = d.get("action_values")
                revenue     = _extract_action(action_vals, "omni_purchase")
                conversions = _extract_action(actions,     "omni_purchase")
                atc         = _extract_action(actions,     "add_to_cart")
                atc_value   = _extract_action(action_vals, "add_to_cart")
                checkout    = _extract_action(actions,     "initiate_checkout")
                impressions = int(d.get("impressions", 0) or 0)
                clicks      = int(d.get("clicks", 0) or 0)
                ctr         = float(d.get("ctr", 0) or 0)
                roas        = round(revenue / spend, 2) if spend > 0 else 0.0

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
                rows_upserted += 1
            return rows_upserted

        except Exception as e:
            err = str(e)
            is_rate_limit = "rate" in err.lower() or "429" in err or "throttle" in err.lower()
            wait = (2 ** attempt) * 5 if is_rate_limit else 2
            print(f"[ingest] chunk error (attempt {attempt}/{MAX_RETRIES}) {account_id} "
                  f"{date_from}→{date_to}: {err}. Waiting {wait}s")
            if attempt < MAX_RETRIES:
                time.sleep(wait)
            else:
                raise


def _covered_campaign_dates(account_id: str, date_from: str, date_to: str) -> set:
    """Return (date, campaign_id) pairs already in campaign_daily_metrics."""
    try:
        resp = (
            supabase.table("campaign_daily_metrics")
            .select("date, campaign_id")
            .eq("account_id", account_id.replace("act_", ""))
            .gte("date", date_from)
            .lte("date", date_to)
            .execute()
        )
        return {(str(r["date"])[:10], r["campaign_id"]) for r in (resp.data or [])}
    except Exception:
        return set()


def _pull_campaign_chunk(account_id: str, date_from: str, date_to: str) -> int:
    """
    Pull campaign-level daily data for one chunk and upsert into campaign_daily_metrics.
    Uses 30-day chunks and level=campaign with time_increment=1.
    """
    from facebook_business.api import FacebookAdsApi
    from facebook_business.adobjects.adaccount import AdAccount
    from app.core.config import settings

    FacebookAdsApi.init(access_token=settings.META_SYSTEM_USER_TOKEN, api_version='v22.0')
    norm_id  = account_id if account_id.startswith("act_") else f"act_{account_id}"
    clean_id = account_id.replace("act_", "")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            insights = AdAccount(norm_id).get_insights(
                fields=[
                    "campaign_id", "campaign_name",
                    "spend", "impressions", "clicks", "ctr",
                    "actions", "action_values",
                ],
                params={
                    "level":          "campaign",
                    "time_range":     {"since": date_from, "until": date_to},
                    "time_increment": 1,
                    "limit":          500,
                },
            )
            rows_upserted = 0
            for row in insights:
                d = row.export_all_data()
                date        = d.get("date_start", "")
                campaign_id = d.get("campaign_id", "")
                if not date or not campaign_id:
                    continue

                actions     = d.get("actions")
                action_vals = d.get("action_values")
                spend       = float(d.get("spend", 0) or 0)
                revenue     = _extract_action(action_vals, "omni_purchase")
                conversions = _extract_action(actions,     "omni_purchase")
                atc         = _extract_action(actions,     "add_to_cart")
                atc_value   = _extract_action(action_vals, "add_to_cart")
                checkout    = _extract_action(actions,     "initiate_checkout")
                impressions = int(d.get("impressions", 0) or 0)
                clicks      = int(d.get("clicks", 0) or 0)
                ctr         = float(d.get("ctr", 0) or 0)
                roas        = round(revenue / spend, 2) if spend > 0 else 0.0

                supabase.table("campaign_daily_metrics").upsert(
                    {
                        "date":          date,
                        "campaign_id":   campaign_id,
                        "campaign_name": d.get("campaign_name", ""),
                        "account_id":    clean_id,
                        "spend":         round(spend, 2),
                        "revenue":       round(revenue, 2),
                        "roas":          roas,
                        "conversions":   round(conversions, 1),
                        "impressions":   impressions,
                        "clicks":        clicks,
                        "ctr":           round(ctr, 2),
                        "atc":           round(atc, 1),
                        "atc_value":     round(atc_value, 2),
                        "checkout":      round(checkout, 1),
                        "synced_at":     datetime.utcnow().isoformat(),
                    },
                    on_conflict="date,campaign_id",
                ).execute()
                rows_upserted += 1
            return rows_upserted

        except Exception as e:
            err = str(e)
            is_rate_limit = "rate" in err.lower() or "429" in err or "throttle" in err.lower()
            wait = (2 ** attempt) * 5 if is_rate_limit else 2
            print(f"[campaign] chunk error (attempt {attempt}/{MAX_RETRIES}) "
                  f"{account_id} {date_from}→{date_to}: {err}. Waiting {wait}s")
            if attempt < MAX_RETRIES:
                time.sleep(wait)
            else:
                raise


class IngestService:

    @staticmethod
    def sync_meta_accounts(ad_account_id: str) -> Dict[str, Any]:
        """Campaign-level sync for the rule engine (populates performance_metrics)."""
        raw_campaigns = meta_service.get_account_metrics(ad_account_id)
        normalized_entities, rows = [], []
        for raw in raw_campaigns:
            ctx = meta_service.normalize_to_ctx(raw)
            normalized_entities.append(ctx)
            rows.append({
                "entity_id":   ctx.entity_id,
                "entity_name": ctx.entity_name,
                "account_id":  ad_account_id,
                "spend":       ctx.m7d.spend,
                "revenue":     ctx.m7d.revenue,
                "roas":        ctx.m7d.roas,
                "conversions": ctx.m7d.conversions,
            })

        try:
            supabase.table("performance_metrics").delete().eq("account_id", ad_account_id).execute()
        except Exception:
            supabase.table("performance_metrics").delete().neq(
                "id", "00000000-0000-0000-0000-000000000000"
            ).execute()

        errors = 0
        for row in rows:
            try:
                supabase.table("performance_metrics").insert(row).execute()
            except Exception as e:
                errors += 1
                print(f"[ingest] insert error {row.get('entity_name')}: {e}")

        suggestions = executor.process_entities(normalized_entities)
        return {
            "entities_synced":   len(rows) - errors,
            "suggestions_fired": len(suggestions),
            "suggestions":       suggestions,
        }

    @staticmethod
    def sync_daily_metrics(
        account_id: str,
        date_from:  str,
        date_to:    str,
        job_id:     Optional[str] = None,
        skip_existing: bool = True,
    ) -> Dict[str, Any]:
        """
        Pull daily account-level data from Meta in 90-day chunks and upsert into daily_metrics.

        skip_existing=True  → skips date ranges already stored (safe for re-runs)
        skip_existing=False → force re-pulls everything (useful for fixing stale data)
        """
        from app.core.config import settings
        if not settings.META_SYSTEM_USER_TOKEN:
            err = "META_SYSTEM_USER_TOKEN not configured"
            if job_id:
                _update_job(job_id, status="failed", error=err)
            return {"error": err, "rows_synced": 0}

        clean_id = account_id.replace("act_", "")
        chunks   = list(_date_chunks(date_from, date_to, CHUNK_DAYS))
        covered  = _covered_dates(clean_id, date_from, date_to) if skip_existing else set()

        if job_id:
            _update_job(job_id, status="running", total_chunks=len(chunks))

        total_rows, done_chunks = 0, 0
        try:
            for chunk_from, chunk_to in chunks:
                # Skip chunk entirely if every day in it is already stored
                if skip_existing and covered:
                    chunk_start = datetime.strptime(chunk_from, "%Y-%m-%d")
                    chunk_end   = datetime.strptime(chunk_to,   "%Y-%m-%d")
                    days_in_chunk = {
                        (chunk_start + timedelta(days=i)).strftime("%Y-%m-%d")
                        for i in range((chunk_end - chunk_start).days + 1)
                    }
                    if days_in_chunk.issubset(covered):
                        done_chunks += 1
                        if job_id:
                            _update_job(job_id, done_chunks=done_chunks)
                        continue

                rows = _pull_chunk(account_id, chunk_from, chunk_to)
                total_rows  += rows
                done_chunks += 1

                if job_id:
                    _update_job(job_id, done_chunks=done_chunks, rows_synced=total_rows)

                # Polite delay between chunks
                if done_chunks < len(chunks):
                    time.sleep(CHUNK_DELAY)

        except Exception as e:
            if job_id:
                _update_job(job_id, status="failed", error=str(e), rows_synced=total_rows)
            return {"account_id": account_id, "rows_synced": total_rows, "error": str(e)}

        if job_id:
            _update_job(job_id, status="completed", rows_synced=total_rows, done_chunks=len(chunks))

        return {
            "account_id": account_id,
            "date_from":  date_from,
            "date_to":    date_to,
            "rows_synced": total_rows,
        }

    @staticmethod
    def sync_campaign_daily_metrics(
        account_id:    str,
        date_from:     str,
        date_to:       str,
        job_id:        Optional[str] = None,
        skip_existing: bool = True,
    ) -> Dict[str, Any]:
        """
        Pull campaign-level daily data in 30-day chunks and store in campaign_daily_metrics.
        Runs after account-level sync so rate limit budget is shared carefully.
        """
        from app.core.config import settings
        from facebook_business.api import FacebookAdsApi
        from facebook_business.adobjects.adaccount import AdAccount

        if not settings.META_SYSTEM_USER_TOKEN:
            return {"error": "META_SYSTEM_USER_TOKEN not configured", "rows_synced": 0}

        clean_id = account_id.replace("act_", "")
        norm_id  = account_id if account_id.startswith("act_") else f"act_{account_id}"

        # Fetch and store current campaign statuses once per account sync
        try:
            FacebookAdsApi.init(access_token=settings.META_SYSTEM_USER_TOKEN, api_version='v22.0')
            camps = AdAccount(norm_id).get_campaigns(fields=["id", "name", "effective_status", "created_time"], params={"limit": 1000})
            for c in camps:
                row = {
                    "id":         c.get("id"),
                    "account_id": clean_id,
                    "name":       c.get("name", ""),
                    "status":     c.get("effective_status", "ACTIVE"),
                    "updated_at": datetime.utcnow().isoformat(),
                }
                if c.get("created_time"):
                    row["created_at"] = c["created_time"]
                supabase.table("campaigns").upsert(row, on_conflict="id").execute()
        except Exception as e:
            print(f"[ingest] failed to sync campaign statuses for {clean_id}: {e}")

        chunks   = list(_date_chunks(date_from, date_to, CAMPAIGN_CHUNK_DAYS))
        covered  = _covered_campaign_dates(clean_id, date_from, date_to) if skip_existing else set()

        total_rows, done_chunks = 0, 0
        try:
            for chunk_from, chunk_to in chunks:
                # Skip chunk if ALL days in it already have campaign data
                if skip_existing and covered:
                    chunk_start = datetime.strptime(chunk_from, "%Y-%m-%d")
                    chunk_end   = datetime.strptime(chunk_to,   "%Y-%m-%d")
                    days_in_chunk = {
                        (chunk_start + timedelta(days=i)).strftime("%Y-%m-%d")
                        for i in range((chunk_end - chunk_start).days + 1)
                    }
                    # Can't easily check all campaigns, so only skip if first day is covered
                    if chunk_from in {d for d, _ in covered}:
                        done_chunks += 1
                        continue

                rows = _pull_campaign_chunk(account_id, chunk_from, chunk_to)
                total_rows  += rows
                done_chunks += 1

                if done_chunks < len(chunks):
                    time.sleep(CHUNK_DELAY)

        except Exception as e:
            return {"account_id": account_id, "rows_synced": total_rows, "error": str(e)}

        return {
            "account_id":  account_id,
            "date_from":   date_from,
            "date_to":     date_to,
            "rows_synced": total_rows,
        }

    @staticmethod
    def sync_all_accounts_daily(date_from: str, date_to: str) -> Dict[str, Any]:
        """
        Sync daily_metrics for ALL mapped META accounts sequentially.
        Sequential (not parallel) to stay within Meta's rate limits.
        """
        try:
            accts_resp = supabase.table("brand_accounts").select("account_id, platform").execute()
            meta_accounts = [
                a["account_id"] for a in (accts_resp.data or [])
                if a.get("platform", "").upper() == "META"
            ]
        except Exception:
            meta_accounts = []

        results = []
        for account_id in meta_accounts:
            result = IngestService.sync_daily_metrics(account_id, date_from, date_to)
            results.append(result)

        return {
            "accounts_synced":  len(meta_accounts),
            "total_rows_synced": sum(r.get("rows_synced", 0) for r in results),
            "details": results,
        }

    @staticmethod
    def sync_adset_daily_metrics(
        campaign_id: str,
        account_id: str,
        date_from: str,
        date_to: str,
        skip_existing: bool = True,
    ) -> Dict[str, Any]:
        """
        Pull adset-level daily data from Meta for a campaign and store in adset_daily_metrics.
        Uses 30-day chunks, level=adset, time_increment=1.
        """
        from facebook_business.api import FacebookAdsApi
        from facebook_business.adobjects.campaign import Campaign
        from app.core.config import settings

        if not settings.META_SYSTEM_USER_TOKEN:
            return {"error": "META_SYSTEM_USER_TOKEN not configured", "rows_synced": 0}

        FacebookAdsApi.init(access_token=settings.META_SYSTEM_USER_TOKEN, api_version='v22.0')
        clean_account = account_id.replace("act_", "")

        # Check what's already stored
        covered: set = set()
        if skip_existing:
            try:
                resp = (
                    supabase.table("adset_daily_metrics")
                    .select("date, adset_id")
                    .eq("campaign_id", campaign_id)
                    .gte("date", date_from)
                    .lte("date", date_to)
                    .execute()
                )
                covered = {(str(r["date"])[:10], r["adset_id"]) for r in (resp.data or [])}
            except Exception:
                covered = set()

        total_rows = 0
        for chunk_from, chunk_to in _date_chunks(date_from, date_to, CAMPAIGN_CHUNK_DAYS):
            if skip_existing and covered:
                if chunk_from in {d for d, _ in covered}:
                    continue

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    insights = Campaign(campaign_id).get_insights(
                        fields=[
                            "adset_id", "adset_name",
                            "spend", "impressions", "clicks", "ctr",
                            "actions", "action_values",
                        ],
                        params={
                            "level": "adset",
                            "time_range": {"since": chunk_from, "until": chunk_to},
                            "time_increment": 1,
                            "limit": 500,
                        },
                    )
                    for row in insights:
                        d = row.export_all_data()
                        date    = d.get("date_start", "")
                        adset_id = d.get("adset_id", "")
                        if not date or not adset_id:
                            continue
                        actions     = d.get("actions")
                        action_vals = d.get("action_values")
                        spend       = float(d.get("spend", 0) or 0)
                        revenue     = _extract_action(action_vals, "omni_purchase")
                        conversions = _extract_action(actions,     "omni_purchase")
                        atc         = _extract_action(actions,     "add_to_cart")
                        atc_value   = _extract_action(action_vals, "add_to_cart")
                        checkout    = _extract_action(actions,     "initiate_checkout")
                        impressions = int(d.get("impressions", 0) or 0)
                        clicks      = int(d.get("clicks", 0) or 0)
                        ctr         = float(d.get("ctr", 0) or 0)
                        roas        = round(revenue / spend, 2) if spend > 0 else 0.0

                        supabase.table("adset_daily_metrics").upsert(
                            {
                                "date":         date,
                                "adset_id":     adset_id,
                                "adset_name":   d.get("adset_name", ""),
                                "campaign_id":  campaign_id,
                                "account_id":   clean_account,
                                "spend":        round(spend, 2),
                                "revenue":      round(revenue, 2),
                                "roas":         roas,
                                "conversions":  round(conversions, 1),
                                "impressions":  impressions,
                                "clicks":       clicks,
                                "ctr":          round(ctr, 2),
                                "atc":          round(atc, 1),
                                "atc_value":    round(atc_value, 2),
                                "checkout":     round(checkout, 1),
                                "synced_at":    datetime.utcnow().isoformat(),
                            },
                            on_conflict="date,adset_id",
                        ).execute()
                        total_rows += 1
                    break
                except Exception as e:
                    err = str(e)
                    is_rate_limit = "rate" in err.lower() or "429" in err or "throttle" in err.lower()
                    wait = (2 ** attempt) * 5 if is_rate_limit else 2
                    print(f"[adset] chunk error (attempt {attempt}/{MAX_RETRIES}) "
                          f"{campaign_id} {chunk_from}→{chunk_to}: {err}")
                    if attempt < MAX_RETRIES:
                        time.sleep(wait)
                    else:
                        return {"campaign_id": campaign_id, "rows_synced": total_rows, "error": err}

            time.sleep(CHUNK_DELAY)

        return {"campaign_id": campaign_id, "date_from": date_from, "date_to": date_to, "rows_synced": total_rows}

    @staticmethod
    def _fetch_ads_metadata(adset_id: str) -> Dict[str, Dict]:
        """
        Fetch creative details + effective_status for ALL ads in an adset
        via a single AdSet.get_ads() API call (replaces the old N*2 per-ad loop).
        Returns dict keyed by ad_id.
        """
        from facebook_business.adobjects.adset import AdSet

        def _safe_dict(obj) -> dict:
            if obj is None:
                return {}
            if hasattr(obj, "export_all_data"):
                return obj.export_all_data() or {}
            if isinstance(obj, dict):
                return obj
            return {}

        def _extract_from_oss(oss: dict) -> tuple:
            """Returns (title, body, dest_url, cta, thumbnail)."""
            title, body, dest_url, cta, thumbnail = "", "", "", "", ""
            # Link-share / SHARE type ads
            ld = _safe_dict(oss.get("link_data"))
            if ld:
                title     = title     or ld.get("name", "")
                body      = body      or ld.get("message", "")
                dest_url  = dest_url  or ld.get("link", "")
                # picture field carries the OG image for SHARE/link-preview ads
                thumbnail = thumbnail or ld.get("picture", "") or ld.get("image_url", "")
                cta_obj   = _safe_dict(ld.get("call_to_action"))
                cta       = cta       or cta_obj.get("type", "")
                if not dest_url:
                    dest_url = _safe_dict(cta_obj.get("value")).get("link", "")
            # Video ads
            vd = _safe_dict(oss.get("video_data"))
            if vd:
                title    = title    or vd.get("title", "")
                body     = body     or vd.get("message", "")
                cta_obj  = _safe_dict(vd.get("call_to_action"))
                cta      = cta      or cta_obj.get("type", "")
                if not dest_url:
                    dest_url = _safe_dict(cta_obj.get("value")).get("link", "")
            # Carousel / multi-share
            md = _safe_dict(oss.get("template_data") or oss.get("multi_share_data"))
            if md:
                body     = body     or md.get("message", "")
                dest_url = dest_url or md.get("link", "")
            return title, body, dest_url, cta, thumbnail

        try:
            ads = AdSet(adset_id).get_ads(
                fields=[
                    "id", "name", "effective_status",
                    "creative{title,body,object_type,thumbnail_url,image_url,"
                    "call_to_action_type,link_url,object_story_spec,"
                    # SHARE type ads store post content behind this ID
                    "effective_object_story_id}",
                ],
                params={"limit": 500},
            )
            result: Dict[str, Dict] = {}
            # Map ad_id → effective_object_story_id for SHARE type ads
            share_story_ids: Dict[str, str] = {}

            for ad in ads:
                d     = _safe_dict(ad)
                ad_id = d.get("id", "")
                if not ad_id:
                    continue
                cr        = _safe_dict(d.get("creative"))
                title     = cr.get("title", "")
                body      = cr.get("body", "")
                obj_type  = cr.get("object_type", "")
                thumbnail = cr.get("thumbnail_url", "") or cr.get("image_url", "")
                image_url = cr.get("image_url", "") or thumbnail
                cta       = cr.get("call_to_action_type", "")
                dest_url  = cr.get("link_url", "")
                oss = _safe_dict(cr.get("object_story_spec"))
                if oss:
                    t2, b2, d2, c2, th2 = _extract_from_oss(oss)
                    title     = title     or t2
                    body      = body      or b2
                    dest_url  = dest_url  or d2
                    cta       = cta       or c2
                    thumbnail = thumbnail or th2
                    image_url = image_url or th2

                result[ad_id] = {
                    "ad_title":        title,
                    "ad_body":         body,
                    "creative_type":   obj_type,
                    "thumbnail_url":   thumbnail,
                    "image_url":       image_url,
                    "call_to_action":  cta,
                    "destination_url": dest_url,
                    "ad_status":       d.get("effective_status", "UNKNOWN"),
                }

                # Queue SHARE ads for a post-content fetch
                story_id = (
                    cr.get("effective_object_story_id", "")
                    or cr.get("object_story_id", "")
                )
                if obj_type == "SHARE":
                    print(f"[creative] SHARE ad={ad_id} story_id={story_id!r} title={title!r} body={body[:30]!r} thumb={bool(thumbnail)}")
                    print(f"[creative] SHARE oss content: {oss}")
                if obj_type == "SHARE" and story_id and (not title or not body):
                    share_story_ids[ad_id] = story_id

            # ── Fetch page post content for all SHARE ads ────────────────────────
            # The System User token can't read page posts directly — need a Page
            # Access Token. Strategy: get page token once per unique page_id, then
            # fetch posts. Falls back to SDK call if page token unavailable.
            if share_story_ids:
                try:
                    import httpx
                    from app.core.config import settings as _settings

                    # Group story IDs by page_id so we fetch page tokens once per page
                    page_stories: Dict[str, List[str]] = {}  # page_id → [story_id, ...]
                    for sid in set(share_story_ids.values()):
                        page_id = sid.split("_")[0] if "_" in sid else ""
                        if page_id:
                            page_stories.setdefault(page_id, []).append(sid)

                    # Fetch page access tokens for each page (system user must be page admin)
                    page_tokens: Dict[str, str] = {}
                    for page_id in page_stories:
                        try:
                            pt_resp = httpx.get(
                                f"https://graph.facebook.com/v22.0/{page_id}",
                                params={
                                    "fields":       "access_token",
                                    "access_token": _settings.META_SYSTEM_USER_TOKEN,
                                },
                                timeout=15,
                            )
                            pt_data = pt_resp.json()
                            page_tokens[page_id] = pt_data.get("access_token", _settings.META_SYSTEM_USER_TOKEN)
                            print(f"[creative] page_token for {page_id}: {'got it' if 'access_token' in pt_data else 'not found, using system token'}")
                        except Exception as e:
                            page_tokens[page_id] = _settings.META_SYSTEM_USER_TOKEN
                            print(f"[creative] page token fetch failed for {page_id}: {e}")

                    # Fetch each story's post content using its page token
                    posts_data: Dict[str, dict] = {}
                    unique_stories = list(set(share_story_ids.values()))
                    for sid in unique_stories:
                        page_id = sid.split("_")[0] if "_" in sid else ""
                        token   = page_tokens.get(page_id, _settings.META_SYSTEM_USER_TOKEN)
                        try:
                            p_resp = httpx.get(
                                f"https://graph.facebook.com/v22.0/{sid}",
                                params={
                                    "fields":       "message,full_picture,attachments{title,description,url_tags}",
                                    "access_token": token,
                                },
                                timeout=15,
                            )
                            p_data = p_resp.json()
                            print(f"[creative] post fetch {sid}: status={p_resp.status_code} keys={list(p_data.keys())}")
                            if "id" in p_data or "message" in p_data:
                                posts_data[sid] = p_data
                        except Exception as e:
                            print(f"[creative] post fetch failed for {sid}: {e}")

                    for ad_id, story_id in share_story_ids.items():
                        post = posts_data.get(story_id, {})
                        if not post:
                            continue
                        post_body = post.get("message", "")
                        post_pic  = post.get("full_picture", "")
                        att_list  = post.get("attachments", {}).get("data", [{}])
                        att       = att_list[0] if att_list else {}
                        att_title = att.get("title", "")
                        att_desc  = att.get("description", "")

                        print(f"[creative] SHARE filled ad={ad_id} body={post_body[:40]!r} title={att_title!r} pic={bool(post_pic)}")

                        if not result[ad_id]["ad_title"]:
                            result[ad_id]["ad_title"] = att_title
                        if not result[ad_id]["ad_body"]:
                            result[ad_id]["ad_body"] = post_body or att_desc
                        if not result[ad_id]["thumbnail_url"] and post_pic:
                            result[ad_id]["thumbnail_url"] = post_pic
                            result[ad_id]["image_url"]     = post_pic

                except Exception as e:
                    print(f"[creative] SHARE post fetch failed: {e}")

            print(f"[creative] batch fetched {len(result)} ads for adset {adset_id}")
            return result
        except Exception as e:
            print(f"[creative] batch fetch failed for adset {adset_id}: {e}")
            return {}

    @staticmethod
    def sync_ad_daily_metrics(
        adset_id: str,
        campaign_id: str,
        account_id: str,
        date_from: str,
        date_to: str,
        skip_existing: bool = True,
    ) -> Dict[str, Any]:
        """
        Pull ad-level daily data from Meta for an adset and store in ad_daily_metrics.
        Also fetches creative details (title, body, type, thumbnail, CTA) per ad.
        """
        from facebook_business.api import FacebookAdsApi
        from facebook_business.adobjects.adset import AdSet
        from app.core.config import settings

        if not settings.META_SYSTEM_USER_TOKEN:
            return {"error": "META_SYSTEM_USER_TOKEN not configured", "rows_synced": 0}

        FacebookAdsApi.init(access_token=settings.META_SYSTEM_USER_TOKEN, api_version='v22.0')
        clean_account = account_id.replace("act_", "")

        covered: set = set()
        if skip_existing:
            try:
                resp = (
                    supabase.table("ad_daily_metrics")
                    .select("date, ad_id")
                    .eq("adset_id", adset_id)
                    .gte("date", date_from)
                    .lte("date", date_to)
                    .execute()
                )
                covered = {(str(r["date"])[:10], r["ad_id"]) for r in (resp.data or [])}
            except Exception:
                covered = set()

        # Collect all insight rows first, then batch-fetch creatives once
        all_insight_rows: List[Dict] = []
        total_rows = 0

        for chunk_from, chunk_to in _date_chunks(date_from, date_to, CAMPAIGN_CHUNK_DAYS):
            if skip_existing and covered:
                if chunk_from in {d for d, _ in covered}:
                    continue

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    insights = AdSet(adset_id).get_insights(
                        fields=[
                            "ad_id", "ad_name",
                            "spend", "impressions", "clicks", "ctr",
                            "actions", "action_values",
                        ],
                        params={
                            "level": "ad",
                            "time_range": {"since": chunk_from, "until": chunk_to},
                            "time_increment": 1,
                            "limit": 500,
                        },
                    )
                    for row in insights:
                        d = row.export_all_data()
                        if d.get("date_start") and d.get("ad_id"):
                            all_insight_rows.append(d)
                    break
                except Exception as e:
                    err = str(e)
                    is_rate_limit = "rate" in err.lower() or "429" in err or "throttle" in err.lower()
                    wait = (2 ** attempt) * 5 if is_rate_limit else 2
                    print(f"[ad] chunk error (attempt {attempt}/{MAX_RETRIES}) "
                          f"{adset_id} {chunk_from}→{chunk_to}: {err}")
                    if attempt < MAX_RETRIES:
                        time.sleep(wait)
                    else:
                        return {"adset_id": adset_id, "rows_synced": total_rows, "error": err}

            time.sleep(CHUNK_DELAY)

        if not all_insight_rows:
            return {"adset_id": adset_id, "date_from": date_from, "date_to": date_to, "rows_synced": 0}

        # Batch-fetch creatives + status for all ads in one API call
        ads_meta = IngestService._fetch_ads_metadata(adset_id)

        # Upsert all rows with creative + status data merged in
        for d in all_insight_rows:
            date  = d.get("date_start", "")
            ad_id = d.get("ad_id", "")
            actions     = d.get("actions")
            action_vals = d.get("action_values")
            spend       = float(d.get("spend", 0) or 0)
            revenue     = _extract_action(action_vals, "omni_purchase")
            conversions = _extract_action(actions,     "omni_purchase")
            atc         = _extract_action(actions,     "add_to_cart")
            atc_value   = _extract_action(action_vals, "add_to_cart")
            checkout    = _extract_action(actions,     "initiate_checkout")
            impressions = int(d.get("impressions", 0) or 0)
            clicks      = int(d.get("clicks", 0) or 0)
            ctr         = float(d.get("ctr", 0) or 0)
            roas        = round(revenue / spend, 2) if spend > 0 else 0.0
            meta        = ads_meta.get(ad_id, {})

            supabase.table("ad_daily_metrics").upsert(
                {
                    "date":            date,
                    "ad_id":           ad_id,
                    "ad_name":         d.get("ad_name", ""),
                    "adset_id":        adset_id,
                    "campaign_id":     campaign_id,
                    "account_id":      clean_account,
                    "spend":           round(spend, 2),
                    "revenue":         round(revenue, 2),
                    "roas":            roas,
                    "conversions":     round(conversions, 1),
                    "impressions":     impressions,
                    "clicks":          clicks,
                    "ctr":             round(ctr, 2),
                    "atc":             round(atc, 1),
                    "atc_value":       round(atc_value, 2),
                    "checkout":        round(checkout, 1),
                    "ad_title":        meta.get("ad_title", ""),
                    "ad_body":         meta.get("ad_body", ""),
                    "creative_type":   meta.get("creative_type", ""),
                    "thumbnail_url":   meta.get("thumbnail_url", ""),
                    "image_url":       meta.get("image_url", ""),
                    "call_to_action":  meta.get("call_to_action", ""),
                    "destination_url": meta.get("destination_url", ""),
                    "ad_status":       meta.get("ad_status", "UNKNOWN"),
                    "synced_at":       datetime.utcnow().isoformat(),
                },
                on_conflict="date,ad_id",
            ).execute()
            total_rows += 1

        return {"adset_id": adset_id, "date_from": date_from, "date_to": date_to, "rows_synced": total_rows}

    @staticmethod
    def backfill_ad_creatives(adset_id: str) -> Dict[str, Any]:
        """
        Fast creative backfill: fetch metadata for all ads in the adset via a single
        API call and update creative/status fields in ad_daily_metrics without
        re-pulling performance data from Meta.
        """
        from app.core.config import settings
        from facebook_business.api import FacebookAdsApi

        if not settings.META_SYSTEM_USER_TOKEN:
            return {"error": "META_SYSTEM_USER_TOKEN not configured", "updated": 0}

        FacebookAdsApi.init(access_token=settings.META_SYSTEM_USER_TOKEN, api_version='v22.0')
        ads_meta = IngestService._fetch_ads_metadata(adset_id)

        if not ads_meta:
            return {"adset_id": adset_id, "updated": 0, "error": "no metadata returned"}

        updated = 0
        for ad_id, meta in ads_meta.items():
            try:
                supabase.table("ad_daily_metrics").update({
                    "ad_title":        meta.get("ad_title", ""),
                    "ad_body":         meta.get("ad_body", ""),
                    "creative_type":   meta.get("creative_type", ""),
                    "thumbnail_url":   meta.get("thumbnail_url", ""),
                    "image_url":       meta.get("image_url", ""),
                    "call_to_action":  meta.get("call_to_action", ""),
                    "destination_url": meta.get("destination_url", ""),
                    "ad_status":       meta.get("ad_status", "UNKNOWN"),
                    "synced_at":       datetime.utcnow().isoformat(),
                }).eq("ad_id", ad_id).execute()
                updated += 1
            except Exception as e:
                print(f"[backfill] update error for ad {ad_id}: {e}")

        return {"adset_id": adset_id, "updated": updated}


# Global instance
ingest_service = IngestService()
