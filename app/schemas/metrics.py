from typing import Optional, List
from pydantic import BaseModel
from datetime import datetime

class AdMetrics(BaseModel):
    """Normalized metrics for a specific window (1d, 7d, 14d, etc.)"""
    spend: float = 0.0
    revenue: float = 0.0
    conversions: float = 0.0
    impressions: int = 0
    clicks: int = 0
    roas: float = 0.0
    cpa: float = 0.0
    ctr: float = 0.0
    cvr: float = 0.0

class EntityContext(BaseModel):
    """The full context for a single campaign/entity passed to rules."""
    entity_id: str
    entity_name: str
    channel: str = "META"
    account_id: str
    
    # Core performance windows
    m1d: AdMetrics
    m3d: AdMetrics
    m7d: AdMetrics
    m14d: AdMetrics
    m30d: AdMetrics
    today: AdMetrics
    
    # Trajectory & Trends
    trajectory_score: float = 0.0
    roas_trend: float = 0.0
    roas_cv_7d: float = 0.0  # Coefficient of variation
    
    # Funnel Health
    atc_rate_7d: float = 0.0
    purchase_rate_7d: float = 0.0
    hook_rate_7d: float = 0.0  # Specific to Video Meta ads
    
    # Budget & Pacing
    current_budget: float = 0.0
    budget_utilization_7d: float = 0.0
    
    # Targets
    target_roas: float = 3.5
    target_cpa: float = 500.0
    
    # Metadata
    age_days: int = 0
    learning_phase: bool = False
    status: str = "ACTIVE"
    hour_of_day: int = 0
