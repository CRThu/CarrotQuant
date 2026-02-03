from fastapi import APIRouter
from api.endpoints import system, market

api_router = APIRouter()

# Register endpoints
api_router.include_router(system.router, tags=["system"])
api_router.include_router(market.router, prefix="/market", tags=["market"])
