from typing import Optional, Dict, Any, List
from app.schemas.metrics import EntityContext

class RuleResult:
    def __init__(self, fired: bool = False, suggestion: Optional[Dict[str, Any]] = None):
        self.fired = fired
        self.suggestion = suggestion

def meta_b01_scale_up(ctx: EntityContext) -> RuleResult:
    """
    META-B01: Scale Up Strong Performer.
    Fires when trajectory confirms sustained outperformance.
    """
    scale_threshold = 1.30
    
    roas_7d_ok = ctx.m7d.roas > (ctx.target_roas * scale_threshold)
    roas_3d_ok = ctx.m3d.roas > (ctx.target_roas * scale_threshold * 0.90)
    not_declining = ctx.trajectory_score > -0.05
    budget_util_ok = ctx.budget_utilization_7d > 0.80
    not_learning = not ctx.learning_phase
    has_spend = ctx.m7d.spend > 500
    
    if all([roas_7d_ok, roas_3d_ok, not_declining, budget_util_ok, not_learning, has_spend]):
        magnitude = 20 if ctx.m7d.roas > ctx.target_roas * 1.5 else 15
        return RuleResult(
            fired=True,
            suggestion={
                "type": "BUDGET_INCREASE",
                "direction": "UP",
                "magnitude": magnitude,
                "rule_id": "META-B01",
                "priority": "P5",
                "reason": f"ROAS 7d ({ctx.m7d.roas:.2f}) is well above target. Performance is stable."
            }
        )
    return RuleResult(fired=False)

def meta_b02_scale_down(ctx: EntityContext) -> RuleResult:
    """
    META-B02: Scale Down Consistent Underperformer.
    """
    cut_threshold = 0.60
    
    roas_7d_bad = ctx.m7d.roas < (ctx.target_roas * cut_threshold)
    roas_3d_bad = ctx.m3d.roas < (ctx.target_roas * cut_threshold * 1.15) if ctx.m3d.spend > 0 else roas_7d_bad
    not_recovering = ctx.trajectory_score < 0.05
    has_spend = ctx.m7d.spend > 2000
    not_new = ctx.age_days > 5

    if all([roas_7d_bad, roas_3d_bad, not_recovering, has_spend, not_new]):
        return RuleResult(
            fired=True,
            suggestion={
                "type": "BUDGET_DECREASE",
                "direction": "DOWN",
                "magnitude": 20,
                "rule_id": "META-B02",
                "priority": "P5",
                "reason": f"ROAS 7d ({ctx.m7d.roas:.2f}) consistently underperforming."
            }
        )
    return RuleResult(fired=False)

def meta_f03_funnel_collapse(ctx: EntityContext) -> RuleResult:
    """
    META-F03: Funnel Collapse Fire Alarm.
    Fires on critical drop in today's performance vs 7d average.
    """
    if ctx.today.spend < (ctx.m7d.spend / 7 * 0.5):
        return RuleResult(fired=False)

    collapse = ctx.today.roas < (ctx.m7d.roas * 0.10)
    
    if collapse and ctx.today.spend > 100:
        return RuleResult(
            fired=True,
            suggestion={
                "type": "EMERGENCY_STOP",
                "direction": "DOWN",
                "magnitude": 100,
                "rule_id": "META-F03",
                "priority": "P1",
                "reason": f"CRITICAL: Funnel collapse detected. Today ROAS ({ctx.today.roas:.2f}) is < 10% of 7d average."
            }
        )
    return RuleResult(fired=False)

# Pipeline of Meta rules ordered by priority (P1 -> P5)
META_RULES_PIPELINE = [
    meta_f03_funnel_collapse,
    meta_b01_scale_up,
    meta_b02_scale_down,
]
