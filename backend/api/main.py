from fastapi import APIRouter
from api.endpoints import system

api_router = APIRouter()

# Register endpoints
api_router.include_router(system.router, tags=["system"])
