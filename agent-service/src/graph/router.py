from src.config import get_settings
from fastapi import APIRouter

graph_router = APIRouter()

@graph_router.post("/process_document")
async def process_document():
    
    return ''