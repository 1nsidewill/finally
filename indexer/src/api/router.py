import os

from fastapi import APIRouter, HTTPException, status, Depends, Request
from fastapi.responses import JSONResponse
from src.auth.user_service import get_current_user
from src.config import get_settings

config = get_settings()
api_router = APIRouter()

@api_router.post("/query")
async def query(user=Depends(get_current_user)):