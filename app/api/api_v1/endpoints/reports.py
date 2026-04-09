from typing import Any
from fastapi import APIRouter
import random

router = APIRouter()

@router.get("/summary")
def get_reports_summary() -> Any:
    return {
        "weekly_summary": {
            "total_spend": round(random.uniform(800000, 2000000), 2),
            "total_revenue": round(random.uniform(2000000, 8000000), 2),
            "roas": round(random.uniform(2.5, 5.0), 2),
            "rules_fired": random.randint(12, 48),
            "suggestions_approved": random.randint(5, 20),
            "suggestions_rejected": random.randint(1, 8),
            "ai_calls_made": random.randint(20, 80),
            "budget_changes_executed": random.randint(3, 15),
        },
        "top_performers": [
            {"entity": "META_ADV+_Purchase", "roas": 4.8, "spend": 245000},
            {"entity": "META_ASC_Broad", "roas": 3.9, "spend": 180000},
            {"entity": "META_RE_Retargeting", "roas": 6.2, "spend": 92000},
        ],
        "alerts": [
            {"type": "WARNING", "msg": "META_PROMO_Flash_Sale frequency > 4.0 — creative fatigue detected"},
            {"type": "INFO", "msg": "Budget pace is 8% below expected for main account"},
            {"type": "SUCCESS", "msg": "3 budget scale-ups executed successfully this week"},
        ]
    }

@router.get("/changelog")
def get_changelog() -> Any:
    actions = ["BUDGET_INCREASE", "BUDGET_DECREASE", "PAUSE", "RESUME"]
    channels = ["META", "META", "META", "GOOGLE"]
    log = []
    for i in range(10):
        log.append({
            "id": f"exec_{i:03d}",
            "action": random.choice(actions),
            "entity": f"META_Campaign_{i}",
            "channel": random.choice(channels),
            "magnitude": random.choice([10, 15, 20, 25]),
            "executed_by": "RuleEngine v5.0",
            "status": random.choice(["SUCCESS", "SUCCESS", "SUCCESS", "FAILED"]),
            "timestamp": f"2026-04-07T{10+i:02d}:15:00Z"
        })
    return log
