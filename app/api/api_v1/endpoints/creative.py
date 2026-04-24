from typing import Any, Dict, List, Optional
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from datetime import datetime, timedelta
from collections import defaultdict
import math
import random

from app.db.supabase import supabase

router = APIRouter()

# ── Scoring weights (must sum to 1.0) ────────────────────────────────────────
# Performance score: formula-based, min-max normalized across brand peers
PERF_WEIGHTS = {
    "roas":            0.30,
    "ctr":             0.20,
    "cpm":             0.15,   # lower is better → inverted
    "hook_rate":       0.15,   # (atc+checkout)/impressions*1000
    "conv_efficiency": 0.10,   # conv/spend*1000, lucky-conv penalised
    "spend":           0.10,   # log-normalised so big budgets don't dominate
}

# AI score weights: emphasises creative quality signals over pure volume
AI_WEIGHTS = {
    "roas":            0.25,
    "ctr":             0.25,
    "cpm":             0.10,
    "hook_rate":       0.25,
    "conv_efficiency": 0.10,
    "spend":           0.05,
}

CATEGORY_GOOD    = 65
CATEGORY_AVERAGE = 35

AI_REASONING_TEMPLATES = {
    "GOOD":    "Strong engagement and ROAS efficiency indicate a high-quality creative driving reliable conversions.",
    "AVERAGE": "Moderate performance across key signals; hook rate or CTR improvements could lift this creative.",
    "BAD":     "Low creative engagement signals suggest ad fatigue or weak hook; consider refreshing copy and visuals.",
}


def _default_dates() -> tuple[str, str]:
    today = datetime.now().strftime("%Y-%m-%d")
    since = (datetime.now() - timedelta(days=29)).strftime("%Y-%m-%d")
    return since, today


# ── Normalisation helpers ─────────────────────────────────────────────────────

def _normalize(values: List[float], invert: bool = False) -> List[float]:
    if not values:
        return values
    mn, mx = min(values), max(values)
    if mx == mn:
        return [50.0] * len(values)
    normed = [(v - mn) / (mx - mn) * 100.0 for v in values]
    return [100.0 - n for n in normed] if invert else normed


def _log_normalize(values: List[float]) -> List[float]:
    return _normalize([math.log1p(v) for v in values])


def _lucky_conv_penalty(spend: float, raw_eff: float) -> float:
    """Reduce conv efficiency score for low-spend creatives (lucky conv guard)."""
    if spend < 1_000:
        return raw_eff * 0.50
    if spend < 5_000:
        return raw_eff * 0.75
    return raw_eff


# ── Performance scoring ───────────────────────────────────────────────────────

def _compute_performance_scores(creatives: List[Dict]) -> List[Dict]:
    if not creatives:
        return creatives

    roas_v   = [c["metrics"]["roas"]       for c in creatives]
    ctr_v    = [c["metrics"]["ctr"]        for c in creatives]
    cpm_v    = [c["metrics"]["cpm"]        for c in creatives]
    hook_v   = [c["metrics"]["hook_rate"]  for c in creatives]
    spend_v  = [c["metrics"]["spend"]      for c in creatives]
    conv_eff_v = [
        _lucky_conv_penalty(
            c["metrics"]["spend"],
            c["metrics"]["conversions"] / c["metrics"]["spend"] * 1000
            if c["metrics"]["spend"] > 0 else 0.0
        )
        for c in creatives
    ]

    roas_n    = _normalize(roas_v)
    ctr_n     = _normalize(ctr_v)
    cpm_n     = _normalize(cpm_v, invert=True)
    hook_n    = _normalize(hook_v)
    conv_n    = _normalize(conv_eff_v)
    spend_n   = _log_normalize(spend_v)

    for i, c in enumerate(creatives):
        bd = {
            "roas":            round(roas_n[i],  1),
            "ctr":             round(ctr_n[i],   1),
            "cpm":             round(cpm_n[i],   1),
            "hook_rate":       round(hook_n[i],  1),
            "conv_efficiency": round(conv_n[i],  1),
            "spend":           round(spend_n[i], 1),
        }
        score = sum(bd[k] * w for k, w in PERF_WEIGHTS.items())
        c["performance_score"] = round(score, 1)
        c["score_breakdown"]   = bd
        c["category"] = (
            "GOOD"    if score >= CATEGORY_GOOD
            else "AVERAGE" if score >= CATEGORY_AVERAGE
            else "BAD"
        )
    return creatives


# ── AI scoring ────────────────────────────────────────────────────────────────

def _ai_score_with_claude(creative: Dict) -> tuple[float, str]:
    """Call Claude API. Returns (score, reasoning). Falls back gracefully."""
    try:
        import anthropic
        from app.core.config import settings
        if not settings.ANTHROPIC_API_KEY:
            raise ValueError("no key")

        client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)
        m = creative["metrics"]
        prompt = (
            f"Analyze this Meta ad creative and score it 0–100.\n\n"
            f"Ad: {creative.get('ad_name', 'Unknown')}\n"
            f"Type: {creative.get('creative_type', 'Unknown')}\n\n"
            f"Metrics:\n"
            f"- ROAS: {m['roas']:.2f}x (industry avg ~2.5x)\n"
            f"- CTR: {m['ctr']:.2f}%\n"
            f"- CPM: ₹{m['cpm']:.0f} (lower is better)\n"
            f"- Hook/1K: {m['hook_rate']:.2f} (ATC+Checkout per 1 000 impressions)\n"
            f"- Conversions: {m['conversions']:.0f}\n"
            f"- Spend: ₹{m['spend']:,.0f}\n"
            f"- Revenue: ₹{m['revenue']:,.0f}\n\n"
            f"Formula performance score: {creative.get('performance_score', 0):.1f}/100\n\n"
            "Consider statistical significance of conversions vs spend, "
            "full-funnel health, and creative type. "
            "Respond in exactly this format:\n"
            "SCORE: [0-100]\n"
            "REASON: [one sentence, max 15 words]"
        )

        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=80,
            messages=[{"role": "user", "content": prompt}],
        )
        text = resp.content[0].text.strip()
        score, reason = 50.0, "AI analysis complete."
        for line in text.splitlines():
            if line.startswith("SCORE:"):
                try:
                    score = max(0.0, min(100.0, float(line[6:].strip())))
                except ValueError:
                    pass
            elif line.startswith("REASON:"):
                reason = line[7:].strip()
        return round(score, 1), reason

    except Exception as e:
        print(f"[creative] claude score error: {e}")
        return _ai_score_fallback(creative)


def _ai_score_fallback(creative: Dict) -> tuple[float, str]:
    """Deterministic rule-based AI approximation when Claude is unavailable."""
    bd = creative.get("score_breakdown", {})
    if not bd:
        return 50.0, AI_REASONING_TEMPLATES["AVERAGE"]

    score = sum(bd.get(k, 50) * w for k, w in AI_WEIGHTS.items())
    # Seeded noise per ad so the same ad always gets the same fallback offset
    rng = random.Random(hash(creative["ad_id"]) & 0xFFFF)
    score = max(0.0, min(100.0, score + rng.uniform(-5, 5)))
    score = round(score, 1)

    cat = (
        "GOOD"    if score >= CATEGORY_GOOD
        else "AVERAGE" if score >= CATEGORY_AVERAGE
        else "BAD"
    )
    return score, AI_REASONING_TEMPLATES[cat]


# ── Data helpers ─────────────────────────────────────────────────────────────

def _aggregate_ad_rows(rows: List[Dict]) -> Dict[str, Dict]:
    agg: Dict = defaultdict(lambda: {
        "spend": 0.0, "revenue": 0.0, "conversions": 0.0,
        "impressions": 0, "clicks": 0, "atc": 0.0, "checkout": 0.0,
        "ctr_sum": 0.0, "ctr_n": 0,
        "ad_name": "", "ad_status": "", "adset_id": "", "campaign_id": "", "account_id": "",
        "ad_title": "", "ad_body": "", "creative_type": "",
        "thumbnail_url": "", "image_url": "", "call_to_action": "", "destination_url": "",
        "min_date": "",
        "created_date": "",   # actual Meta created_time, populated from _fetch_ads_from_meta
    })
    for r in rows:
        aid = r["ad_id"]
        agg[aid]["spend"]       += float(r.get("spend")       or 0)
        agg[aid]["revenue"]     += float(r.get("revenue")     or 0)
        agg[aid]["conversions"] += float(r.get("conversions") or 0)
        agg[aid]["impressions"] += int  (r.get("impressions") or 0)
        agg[aid]["clicks"]      += int  (r.get("clicks")      or 0)
        agg[aid]["atc"]         += float(r.get("atc")         or 0)
        agg[aid]["checkout"]    += float(r.get("checkout")    or 0)
        ctr = float(r.get("ctr") or 0)
        if ctr > 0:
            agg[aid]["ctr_sum"] += ctr
            agg[aid]["ctr_n"]   += 1
        for field in ("ad_name", "ad_status", "adset_id", "campaign_id", "account_id",
                      "ad_title", "ad_body", "creative_type",
                      "thumbnail_url", "image_url", "call_to_action", "destination_url"):
            val = r.get(field) or ""
            if val:
                agg[aid][field] = val
        # Track earliest date seen (proxy for launch date in this range)
        date_val = str(r.get("date") or "")[:10]
        if date_val and (not agg[aid]["min_date"] or date_val < agg[aid]["min_date"]):
            agg[aid]["min_date"] = date_val
        # Carry through the actual Meta created_time when available (_created_date set by _fetch_ads_from_meta)
        cd = r.get("_created_date") or ""
        if cd and not agg[aid]["created_date"]:
            agg[aid]["created_date"] = cd
    return agg


def _build_creative(ad_id: str, m: Dict, campaign_status_map: Dict, campaign_names: Dict, date_from: str = "") -> Dict:
    sp  = round(m["spend"], 2)
    rev = round(m["revenue"], 2)
    imp = m["impressions"]
    clk = m["clicks"]
    atc = round(m["atc"], 1)
    chk = round(m["checkout"], 1)

    cpm       = round(sp / imp * 1000, 2)       if imp > 0 else 0.0
    ctr       = round(m["ctr_sum"] / m["ctr_n"], 2) if m["ctr_n"] > 0 else 0.0
    roas      = round(rev / sp, 2)              if sp > 0  else 0.0
    hook_rate = round((atc + chk) / imp * 1000, 4) if imp > 0 else 0.0

    campaign_id     = m["campaign_id"]
    campaign_status = campaign_status_map.get(campaign_id, "UNKNOWN")
    ad_status       = m["ad_status"] or "UNKNOWN"
    is_active       = ad_status == "ACTIVE"
    active_priority = is_active and campaign_status == "ACTIVE"

    return {
        "ad_id":            ad_id,
        "ad_name":          m["ad_name"],
        "ad_status":        ad_status,
        "adset_id":         m["adset_id"],
        "campaign_id":      campaign_id,
        "campaign_status":  campaign_status,
        "is_active":        is_active,
        "active_priority":  active_priority,
        "creative_type":    m["creative_type"] or "UNKNOWN",
        "thumbnail_url":    m["thumbnail_url"],
        "image_url":        m["image_url"],
        "ad_title":         m["ad_title"],
        "ad_body":          m["ad_body"],
        "call_to_action":   m["call_to_action"],
        "destination_url":  m["destination_url"],
        "campaign_name":    campaign_names.get(campaign_id, ""),
        # Use Meta created_time if available, else fall back to first seen in metrics
        "first_seen_date":  m.get("created_date") or m.get("min_date", ""),
        # True when the ad started running AFTER the date_from → genuinely new in this range
        "is_new_in_range":  bool(
            (m.get("created_date") or m.get("min_date", "")) > date_from
        ) if date_from else False,
        "metrics": {
            "spend":       sp,
            "revenue":     rev,
            "roas":        roas,
            "ctr":         ctr,
            "cpm":         cpm,
            "hook_rate":   hook_rate,
            "conversions": round(m["conversions"], 1),
            "impressions": imp,
            "clicks":      clk,
            "atc":         atc,
            "checkout":    chk,
        },
        # placeholders filled later
        "performance_score": 0.0,
        "ai_score":          0.0,
        "score_gap":         0.0,
        "category":          "AVERAGE",
        "score_breakdown":   {},
        "ai_reasoning":      "",
        "rank":              0,
    }


def _store_score(c: Dict, brand_id: str, date_from: str, date_to: str) -> None:
    try:
        supabase.table("creative_scores").upsert({
            "ad_id":             c["ad_id"],
            "brand_id":          brand_id,
            "date_from":         date_from,
            "date_to":           date_to,
            "performance_score": c["performance_score"],
            "ai_score":          c["ai_score"],
            "score_gap":         c["score_gap"],
            "category":          c["category"],
            "spend":             c["metrics"]["spend"],
            "roas":              c["metrics"]["roas"],
            "ctr":               c["metrics"]["ctr"],
            "cpm":               c["metrics"]["cpm"],
            "hook_rate":         c["metrics"]["hook_rate"],
            "conversions":       c["metrics"]["conversions"],
            "metric_scores":     c["score_breakdown"],
            "ai_reasoning":      c["ai_reasoning"],
            "analyzed_at":       datetime.utcnow().isoformat(),
        }, on_conflict="ad_id,brand_id,date_from,date_to").execute()
    except Exception as e:
        print(f"[creative] score store error for {c['ad_id']}: {e}")


def _apply_cached_score(c: Dict, s: Dict) -> None:
    c["performance_score"] = float(s.get("performance_score") or 0)
    c["ai_score"]          = float(s.get("ai_score")          or 0)
    c["score_gap"]         = float(s.get("score_gap")         or 0)
    c["category"]          = s.get("category", "AVERAGE")
    c["score_breakdown"]   = s.get("metric_scores") or {}
    c["ai_reasoning"]      = s.get("ai_reasoning")  or ""


# ── Meta API fallback ────────────────────────────────────────────────────────

def _safe_d(obj) -> dict:
    """Safely convert any Meta SDK object or dict to a plain dict."""
    if obj is None:
        return {}
    if hasattr(obj, "export_all_data"):
        return obj.export_all_data() or {}
    if isinstance(obj, dict):
        return obj
    return {}


def _extract_thumb(cr: dict) -> tuple[str, str]:
    """
    Return (thumbnail_url, image_url) from a creative dict.
    Tries direct fields first, then object_story_spec sub-objects.
    Mirrors the logic in IngestService._fetch_ads_metadata.
    """
    thumb = cr.get("thumbnail_url", "") or ""
    img   = cr.get("image_url", "") or ""

    oss = _safe_d(cr.get("object_story_spec"))
    if oss:
        # Link-share / SHARE
        ld = _safe_d(oss.get("link_data"))
        if ld:
            thumb = thumb or ld.get("picture", "") or ld.get("image_url", "") or ""
            img   = img   or ld.get("picture", "") or ld.get("image_url", "") or ""

        # Video
        vd = _safe_d(oss.get("video_data"))
        if vd:
            thumb = thumb or vd.get("thumbnail_url", "") or ""

        # Carousel / template
        td = _safe_d(oss.get("template_data") or oss.get("multi_share_data"))
        if td:
            for child in (td.get("child_attachments") or []):
                cd = _safe_d(child)
                thumb = thumb or cd.get("image_url", "") or cd.get("picture", "") or ""
                img   = img   or cd.get("image_url", "") or cd.get("picture", "") or ""
                if thumb:
                    break

    # Fall back: if only one is set, use it for both
    thumb = thumb or img
    img   = img   or thumb
    return thumb, img


def _creative_type_from_obj(obj_type: str) -> str:
    t = (obj_type or "").upper()
    if t in ("VIDEO", "VIDEO_INLINE"):       return "VIDEO"
    if t in ("CAROUSEL", "TEMPLATE"):        return "CAROUSEL"
    if t in ("PHOTO", "IMAGE"):              return "IMAGE"
    if t in ("SHARE", "LINK", "LINK_SHARE"): return "LINK"
    return t or "IMAGE"


def _fetch_ads_from_meta(
    account_ids: List[str],
    date_from: str,
    date_to: str,
) -> List[Dict]:
    """
    Pull ad-level daily insights from Meta for all accounts when
    ad_daily_metrics has no data for the requested range.
    Also fetches creative details (thumbnail, type, title) and caches
    everything back into ad_daily_metrics for fast future reads.
    """
    from facebook_business.api import FacebookAdsApi
    from facebook_business.adobjects.adaccount import AdAccount
    from app.services.meta import _extract_action
    from app.core.config import settings

    if not settings.META_SYSTEM_USER_TOKEN:
        return []

    FacebookAdsApi.init(access_token=settings.META_SYSTEM_USER_TOKEN, api_version="v22.0")

    all_rows: List[Dict] = []

    for account_id in account_ids:
        norm_id   = account_id if account_id.startswith("act_") else f"act_{account_id}"
        clean_id  = account_id.replace("act_", "")
        synced_at = datetime.utcnow().isoformat()

        try:
            # ── 1. Ad-level daily performance (all ads, all statuses) ──────
            insights = AdAccount(norm_id).get_insights(
                fields=[
                    "ad_id", "ad_name", "campaign_id", "adset_id",
                    "spend", "impressions", "clicks", "ctr",
                    "actions", "action_values",
                ],
                params={
                    "level":          "ad",
                    "time_range":     {"since": date_from, "until": date_to},
                    "time_increment": 1,
                    "limit":          500,
                },
            )

            ad_ids_seen: set = set()
            insight_rows: List[Dict] = []
            for row in insights:
                d     = row.export_all_data()
                ad_id = d.get("ad_id", "")
                date  = d.get("date_start", "")
                if not ad_id or not date:
                    continue
                ad_ids_seen.add(ad_id)
                spend       = float(d.get("spend", 0) or 0)
                revenue     = _extract_action(d.get("action_values"), "omni_purchase")
                conversions = _extract_action(d.get("actions"),       "omni_purchase")
                atc         = _extract_action(d.get("actions"),       "add_to_cart")
                atc_value   = _extract_action(d.get("action_values"), "add_to_cart")
                checkout    = _extract_action(d.get("actions"),       "initiate_checkout")
                insight_rows.append({
                    "date":            date,
                    "ad_id":           ad_id,
                    "ad_name":         d.get("ad_name", ""),
                    "adset_id":        d.get("adset_id", ""),
                    "campaign_id":     d.get("campaign_id", ""),
                    "account_id":      clean_id,
                    "spend":           round(spend, 2),
                    "revenue":         round(revenue, 2),
                    "roas":            round(revenue / spend, 2) if spend > 0 else 0.0,
                    "conversions":     round(conversions, 1),
                    "impressions":     int(d.get("impressions", 0) or 0),
                    "clicks":          int(d.get("clicks", 0) or 0),
                    "ctr":             round(float(d.get("ctr", 0) or 0), 2),
                    "atc":             round(atc, 1),
                    "atc_value":       round(atc_value, 2),
                    "checkout":        round(checkout, 1),
                    "ad_title": "", "ad_body": "", "creative_type": "",
                    "thumbnail_url": "", "image_url": "",
                    "call_to_action": "", "destination_url": "",
                    "ad_status": "UNKNOWN",
                    "synced_at": synced_at,
                })

            # ── 2. Creative details + created_time for all ads ─────────────
            ad_meta: Dict[str, Dict] = {}
            if insight_rows:
                try:
                    ads_resp = AdAccount(norm_id).get_ads(
                        fields=[
                            "id", "name", "effective_status", "created_time",
                            "creative{title,body,object_type,"
                            "thumbnail_url,image_url,"
                            "call_to_action_type,link_url,"
                            "object_story_spec}",
                        ],
                        params={"limit": 500},
                    )
                    for ad in ads_resp:
                        ad_d = _safe_d(ad)
                        aid  = ad_d.get("id", "")
                        if not aid:
                            continue
                        cr            = _safe_d(ad_d.get("creative"))
                        thumb, img    = _extract_thumb(cr)
                        created_time  = ad_d.get("created_time", "")
                        # Normalise created_time to YYYY-MM-DD
                        created_date  = str(created_time)[:10] if created_time else ""

                        ad_meta[aid] = {
                            "ad_title":       cr.get("title", "") or ad_d.get("name", ""),
                            "ad_body":        cr.get("body", ""),
                            "creative_type":  _creative_type_from_obj(cr.get("object_type", "")),
                            "thumbnail_url":  thumb,
                            "image_url":      img,
                            "call_to_action": cr.get("call_to_action_type", ""),
                            "destination_url": cr.get("link_url", ""),
                            "ad_status":      ad_d.get("effective_status", "UNKNOWN"),
                            "created_date":   created_date,
                        }
                except Exception as e:
                    print(f"[creative] get_ads error for {clean_id}: {e}")

            # Merge creative metadata into insight rows
            for r in insight_rows:
                meta = ad_meta.get(r["ad_id"])
                if meta:
                    for k, v in meta.items():
                        if k == "created_date":
                            # Store actual creation date for the analysis layer to use
                            r["_created_date"] = v
                        elif v:
                            r[k] = v

            # ── 3. Cache performance + creative data to ad_daily_metrics ───
            if insight_rows:
                # Strip the _created_date helper field before DB insert
                db_rows = [{k: v for k, v in r.items() if not k.startswith("_")} for r in insight_rows]
                try:
                    for i in range(0, len(db_rows), 200):
                        supabase.table("ad_daily_metrics").upsert(
                            db_rows[i:i + 200],
                            on_conflict="date,ad_id",
                        ).execute()
                except Exception as e:
                    print(f"[creative] ad_daily_metrics cache error: {e}")

            all_rows.extend(insight_rows)

        except Exception as e:
            print(f"[creative] Meta ad insights error for {account_id}: {e}")

    return all_rows


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/analysis")
def get_creative_analysis(
    background_tasks: BackgroundTasks,
    brand_id:         str           = Query(...),
    date_from:        Optional[str] = Query(default=None),
    date_to:          Optional[str] = Query(default=None),
    force_reanalyze:  bool          = Query(default=False),
) -> Any:
    """
    Full creative analysis for a brand.
    Returns ranked creatives with performance + AI scores.
    Scores are cached in creative_scores for 6 hours; force_reanalyze bypasses the cache.
    """
    if not date_from or not date_to:
        date_from, date_to = _default_dates()

    try:
        # 1. Brand accounts
        acct_resp  = supabase.table("brand_accounts").select("account_id").eq("brand_id", brand_id).execute()
        account_ids = [a["account_id"].replace("act_", "") for a in acct_resp.data or []]
        if not account_ids:
            return {"creatives": [], "summary": {}, "date_from": date_from, "date_to": date_to}

        # 2. Ad daily metrics for date range
        rows_resp = (
            supabase.table("ad_daily_metrics")
            .select(
                "date, ad_id, ad_name, ad_status, adset_id, campaign_id, account_id, "
                "spend, revenue, roas, conversions, impressions, clicks, ctr, "
                "atc, atc_value, checkout, "
                "ad_title, ad_body, creative_type, thumbnail_url, image_url, "
                "call_to_action, destination_url"
            )
            .in_("account_id", account_ids)
            .gte("date", date_from)
            .lte("date", date_to)
            .execute()
        )
        rows = rows_resp.data or []
        synced_from_meta = False

        # If no local data, pull from Meta API and cache it
        if not rows:
            print(f"[creative] no ad_daily_metrics for brand {brand_id} {date_from}→{date_to} — pulling from Meta")
            rows = _fetch_ads_from_meta(account_ids, date_from, date_to)
            synced_from_meta = bool(rows)

        if not rows:
            return {"creatives": [], "summary": {}, "date_from": date_from, "date_to": date_to,
                    "synced_from_meta": False}

        # 3. Aggregate per ad_id
        agg = _aggregate_ad_rows(rows)

        # 4. Campaign statuses + names
        campaign_ids = list({m["campaign_id"] for m in agg.values() if m["campaign_id"]})
        campaign_status_map: Dict[str, str] = {}
        campaign_names: Dict[str, str] = {}
        if campaign_ids:
            # Primary source: campaigns table has both status and name
            cr = supabase.table("campaigns").select("id, status, name").in_("id", campaign_ids).execute()
            for c in cr.data or []:
                cid = c.get("id", "")
                if cid:
                    campaign_status_map[cid] = c.get("status") or "UNKNOWN"
                    if c.get("name"):
                        campaign_names[cid] = c["name"]
            # Fallback: campaign_daily_metrics for any names still missing
            missing = [cid for cid in campaign_ids if cid not in campaign_names]
            if missing:
                cn_resp = (
                    supabase.table("campaign_daily_metrics")
                    .select("campaign_id, campaign_name")
                    .in_("campaign_id", missing)
                    .limit(500)
                    .execute()
                )
                for row in cn_resp.data or []:
                    cid = row.get("campaign_id", "")
                    if cid and cid not in campaign_names and row.get("campaign_name"):
                        campaign_names[cid] = row["campaign_name"]

        # 5. Build creative objects; exclude zero-spend
        all_creatives = [
            _build_creative(ad_id, m, campaign_status_map, campaign_names, date_from)
            for ad_id, m in agg.items()
            if m["spend"] > 0
        ]

        if not all_creatives:
            return {"creatives": [], "summary": {}, "date_from": date_from, "date_to": date_to}

        # 5b. Trigger background thumbnail backfill for any ads missing creative data
        missing_thumb_ads = [
            c for c in all_creatives
            if not c.get("thumbnail_url") and not c.get("image_url")
        ]
        missing_thumb_adsets = {c["adset_id"] for c in missing_thumb_ads if c.get("adset_id")}
        if missing_thumb_adsets:
            from app.services.ingest import IngestService
            for adset_id in list(missing_thumb_adsets)[:10]:   # cap at 10 per request
                background_tasks.add_task(IngestService.backfill_ad_creatives, adset_id)
        missing_thumbnails_count = len(missing_thumb_ads)

        # 6. Load cached scores (skip if force_reanalyze)
        cached: Dict[str, Dict] = {}
        if not force_reanalyze:
            cutoff = (datetime.utcnow() - timedelta(hours=6)).isoformat()
            sc_resp = (
                supabase.table("creative_scores")
                .select("*")
                .eq("brand_id", brand_id)
                .eq("date_from", date_from)
                .eq("date_to", date_to)
                .gte("analyzed_at", cutoff)
                .execute()
            )
            cached = {s["ad_id"]: s for s in sc_resp.data or []}

        # 7. Compute performance scores for uncached creatives
        uncached = [c for c in all_creatives if c["ad_id"] not in cached]
        if uncached:
            uncached = _compute_performance_scores(uncached)
            for c in uncached:
                ai_score, ai_reason = _ai_score_with_claude(c)
                c["ai_score"]    = round(ai_score, 1)
                c["ai_reasoning"] = ai_reason
                c["score_gap"]   = round(abs(c["performance_score"] - ai_score), 1)
                _store_score(c, brand_id, date_from, date_to)

        # Build lookup for uncached results
        uncached_map = {c["ad_id"]: c for c in uncached}

        # 8. Merge cached + fresh scores onto all_creatives
        for c in all_creatives:
            if c["ad_id"] in cached:
                _apply_cached_score(c, cached[c["ad_id"]])
            elif c["ad_id"] in uncached_map:
                src = uncached_map[c["ad_id"]]
                c["performance_score"] = src["performance_score"]
                c["ai_score"]          = src["ai_score"]
                c["score_gap"]         = src["score_gap"]
                c["category"]          = src["category"]
                c["score_breakdown"]   = src["score_breakdown"]
                c["ai_reasoning"]      = src["ai_reasoning"]

        # 9. Sort: active_priority first, then is_active, then performance_score desc
        all_creatives.sort(key=lambda x: (
            -int(x["active_priority"]),
            -int(x["is_active"]),
            -x["performance_score"],
        ))
        for i, c in enumerate(all_creatives):
            c["rank"] = i + 1

        # 10. Summary
        active   = [c for c in all_creatives if c["is_active"]]
        inactive = [c for c in all_creatives if not c["is_active"]]
        good     = [c for c in all_creatives if c["category"] == "GOOD"]
        avg_cat  = [c for c in all_creatives if c["category"] == "AVERAGE"]
        bad      = [c for c in all_creatives if c["category"] == "BAD"]

        active_avg_score   = round(sum(c["performance_score"] for c in active)   / len(active),   1) if active   else 0.0
        inactive_avg_score = round(sum(c["performance_score"] for c in inactive) / len(inactive), 1) if inactive else 0.0

        summary = {
            "total":               len(all_creatives),
            "good_count":          len(good),
            "average_count":       len(avg_cat),
            "bad_count":           len(bad),
            "active_count":        len(active),
            "inactive_count":      len(inactive),
            "active_avg_score":    active_avg_score,
            "inactive_avg_score":  inactive_avg_score,
            "score_diff":          round(active_avg_score - inactive_avg_score, 1),
        }

        return {
            "creatives":               all_creatives,
            "summary":                 summary,
            "date_from":               date_from,
            "date_to":                 date_to,
            "brand_id":                brand_id,
            "synced_from_meta":        synced_from_meta,
            "missing_thumbnails_count": missing_thumbnails_count,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/reanalyze/{ad_id}")
def reanalyze_creative(
    ad_id:     str,
    brand_id:  str           = Query(...),
    date_from: Optional[str] = Query(default=None),
    date_to:   Optional[str] = Query(default=None),
) -> Any:
    """
    Clear the cached score for one creative so the next analysis call
    fetches fresh performance + AI scores for it.
    """
    if not date_from or not date_to:
        date_from, date_to = _default_dates()
    try:
        supabase.table("creative_scores").delete() \
            .eq("ad_id",    ad_id) \
            .eq("brand_id", brand_id) \
            .eq("date_from", date_from) \
            .eq("date_to",   date_to) \
            .execute()
        return {"status": "cleared", "ad_id": ad_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/reanalyze-all")
def reanalyze_all(
    brand_id:  str           = Query(...),
    date_from: Optional[str] = Query(default=None),
    date_to:   Optional[str] = Query(default=None),
) -> Any:
    """Clear all cached scores for a brand's date range."""
    if not date_from or not date_to:
        date_from, date_to = _default_dates()
    try:
        supabase.table("creative_scores").delete() \
            .eq("brand_id", brand_id) \
            .eq("date_from", date_from) \
            .eq("date_to",   date_to) \
            .execute()
        return {"status": "cleared", "brand_id": brand_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
