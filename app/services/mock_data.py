import random
from datetime import datetime
from typing import List
from app.schemas.metrics import EntityContext, AdMetrics

def generate_mock_metrics(low: float, high: float) -> AdMetrics:
    spend = random.uniform(low, high)
    roas = random.uniform(1.8, 5.5)
    revenue = spend * roas
    convs = spend / random.uniform(20, 100)
    
    return AdMetrics(
        spend=round(spend, 2),
        revenue=round(revenue, 2),
        roas=round(roas, 2),
        conversions=round(convs, 1),
        impressions=int(spend * 50),
        clicks=int(spend * 2),
        ctr=round(random.uniform(1.5, 3.5), 2),
        cvr=round(random.uniform(2.0, 8.0), 2)
    )

def get_mock_meta_entities() -> List[EntityContext]:
    """Generates 5 realistic Meta ad entities for testing."""
    entities = []
    names = [
        "META_ADV+_Purchase_Prospecting",
        "META_RE_Engagement_Retargeting",
        "META_CAT_DABA_Broad",
        "META_ASC_High_Value_LAL",
        "META_PROMO_Flash_Sale"
    ]
    
    for i, name in enumerate(names):
        m7_spend = random.uniform(2000, 15000)
        entities.append(EntityContext(
            entity_id=f"act_123456789_c_{i}",
            entity_name=name,
            account_id="act_123456789",
            m1d=generate_mock_metrics(100, 1000),
            m3d=generate_mock_metrics(300, 3000),
            m7d=generate_mock_metrics(700, 7000),
            m14d=generate_mock_metrics(1400, 14000),
            m30d=generate_mock_metrics(3000, 30000),
            today=generate_mock_metrics(50, 500),
            trajectory_score=random.uniform(-0.2, 0.4),
            current_budget=random.choice([1000, 2500, 5000, 10000]),
            budget_utilization_7d=random.uniform(0.6, 1.1),
            target_roas=3.0,
            age_days=random.randint(10, 150)
        ))
        
    return entities
