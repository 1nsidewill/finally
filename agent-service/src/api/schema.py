from pydantic import BaseModel, Field
from typing import List

class PropertyItem(BaseModel):
    rank: int = Field(..., description="추천 순위 (1이 가장 높은 순위)")
    title: str
    url: str
    img_url: str
    price: int  # 정수형으로 변경
    odo: str = Field(..., description="주행거리 (예: 46000km, 34000키로)")
    content: str
    match_summary: str
    hash_tags: List[str] = Field(..., description="해시태그 목록 (예: ['#아크라포빅 풀배기', '#좌꿍', '#정비에 미치다', '#올바라시', '#네고가능'])")

# LangChain structured output을 위한 클래스
class PropertyItems(BaseModel):
    items: List[PropertyItem] = Field(..., description="매물 아이템 목록")
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "items": [
                    {
                        "rank": 1,
                        "title": "야마하 R3 2018",
                        "url": "https://example.com/listings/13579",
                        "img_url": "https://example.com/images/r3.jpg",
                        "price": 4600000,
                        "odo": "8000km",
                        "content": "2018년식 야마하 R3 오토바이입니다. 주행거리 8,000km, 단독 사고 없음, 초보 라이더에게 적합한 모델입니다.",
                        "match_summary": "입문용으로 최적, 저렴한 유지비",
                        "hash_tags": ["#야마하", "#R3", "#초보자용", "#저주행", "#깔끔매물"]
                    }
                ]
            }
        }
    }

class QueryRequest(BaseModel):
    question: str
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "question": "R6인데 초보자용"
            }
        }
    }

class QueryResponse(BaseModel):
    result: List[PropertyItem] = Field(..., description="검색 결과 매물 목록")
    message: str = Field(default="성공적으로 검색되었습니다", description="응답 메시지")
    success: bool = Field(default=True, description="요청 성공 여부")
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "success": True,
                "message": "성공적으로 검색되었습니다",
                "result": [
                    {
                        "rank": 1,
                        "title": "야마하 R3 2018",
                        "url": "https://example.com/listings/13579",
                        "img_url": "https://example.com/images/r3.jpg",
                        "price": 4600000,
                        "odo": "8000km",
                        "content": "2018년식 야마하 R3 오토바이입니다. 주행거리 8,000km, 단독 사고 없음, 초보 라이더에게 적합한 모델입니다.",
                        "match_summary": "입문용으로 최적, 저렴한 유지비",
                        "hash_tags": ["#야마하", "#R3", "#초보자용", "#저주행", "#깔끔매물"]
                    }
                ]
            }
        }
    }