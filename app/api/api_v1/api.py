from fastapi import APIRouter
from app.api.api_v1.endpoints import dashboard, analytics, creative, budget, reports, brands, webhooks

api_router = APIRouter()
api_router.include_router(dashboard.router, prefix="/dashboard", tags=["dashboard"])
api_router.include_router(analytics.router, prefix="/analytics", tags=["analytics"])
api_router.include_router(creative.router, prefix="/creative", tags=["creative"])
api_router.include_router(budget.router, prefix="/budget", tags=["budget"])
api_router.include_router(reports.router, prefix="/reports", tags=["reports"])
api_router.include_router(brands.router, prefix="/brands", tags=["brands"])
api_router.include_router(webhooks.router, prefix="/webhooks", tags=["webhooks"])
