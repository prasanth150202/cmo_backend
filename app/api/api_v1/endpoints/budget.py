from typing import Any
from fastapi import APIRouter
import random

router = APIRouter()

ACCOUNTS = [
    {"id": "act_123456789", "name": "Digifyce - Main", "monthly_cap": 1500000},
    {"id": "act_987654321", "name": "Digifyce - Retargeting", "monthly_cap": 500000},
]

@router.get("/pace")
def get_budget_pace() -> Any:
    accounts = []
    for acct in ACCOUNTS:
        cap = acct["monthly_cap"]
        day_of_month = 7
        days_in_month = 30
        expected_spend = cap * (day_of_month / days_in_month)
        actual_spend = expected_spend * random.uniform(0.7, 1.3)
        utilization = actual_spend / cap
        
        pace_status = (
            "OVERPACING" if utilization > (day_of_month / days_in_month) * 1.15
            else "UNDERPACING" if utilization < (day_of_month / days_in_month) * 0.85
            else "ON_PACE"
        )
        
        projected_eom = actual_spend * (days_in_month / day_of_month)
        
        accounts.append({
            "account_id": acct["id"],
            "account_name": acct["name"],
            "monthly_cap": cap,
            "mtd_spend": round(actual_spend, 2),
            "expected_spend": round(expected_spend, 2),
            "utilization_pct": round(utilization * 100, 1),
            "projected_eom_spend": round(projected_eom, 2),
            "remaining_budget": round(cap - actual_spend, 2),
            "pace_status": pace_status,
            "daily_budget_remaining": round((cap - actual_spend) / (days_in_month - day_of_month), 2),
        })
    return {"accounts": accounts, "total_cap": sum(a["monthly_cap"] for a in ACCOUNTS)}


@router.get("/exhaustion-risk")
def get_exhaustion_risk() -> Any:
    campaigns = [
        {"name": "META_ADV+_Purchase", "daily_spend": 45000, "daily_budget": 50000, "days_left": 23},
        {"name": "META_RE_Engagement", "daily_spend": 8200, "daily_budget": 8000, "days_left": 23},
        {"name": "META_ASC_Broad", "daily_spend": 12000, "daily_budget": 15000, "days_left": 23},
    ]
    results = []
    for c in campaigns:
        util = c["daily_spend"] / c["daily_budget"]
        projected_remaining = (c["daily_budget"] - c["daily_spend"]) * c["days_left"]
        results.append({**c, "utilization": round(util, 2), "projected_remaining": round(projected_remaining, 2),
                        "risk": "HIGH" if util > 1.0 else "MEDIUM" if util > 0.9 else "LOW"})
    return results
