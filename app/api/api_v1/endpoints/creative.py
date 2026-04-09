from typing import Any, List, Dict
from fastapi import APIRouter
import random

router = APIRouter()

CREATIVE_NAMES = [
    "UGC_Testimonial_V3", "Product_Demo_30s", "Before_After_Q1",
    "Founder_Story_V2", "Flash_Sale_Static", "Carousel_Benefits_V5",
    "Reels_Hook_V1", "DPA_Dynamic_Feed"
]

@router.get("/status")
def get_creative_fatigue_status() -> Any:
    creatives = []
    for name in CREATIVE_NAMES:
        freq = round(random.uniform(1.2, 5.5), 2)
        ctr_drop = round(random.uniform(-0.6, 0.1), 2)
        age = random.randint(3, 45)
        
        if freq > 4.0 or ctr_drop < -0.3:
            fatigue_status = "FATIGUED"
        elif freq > 2.8 or ctr_drop < -0.1:
            fatigue_status = "WARNING"
        else:
            fatigue_status = "HEALTHY"

        creatives.append({
            "creative_name": name,
            "frequency_7d": freq,
            "ctr_trend": ctr_drop,
            "age_days": age,
            "impressions": int(random.uniform(50000, 800000)),
            "spend": round(random.uniform(5000, 80000), 2),
            "fatigue_status": fatigue_status,
            "recommendation": (
                "ROTATE_NOW" if fatigue_status == "FATIGUED"
                else "MONITOR" if fatigue_status == "WARNING"
                else "KEEP_RUNNING"
            )
        })
    
    return {
        "creatives": sorted(creatives, key=lambda x: x["frequency_7d"], reverse=True),
        "fatigued_count": sum(1 for c in creatives if c["fatigue_status"] == "FATIGUED"),
        "warning_count": sum(1 for c in creatives if c["fatigue_status"] == "WARNING"),
    }
