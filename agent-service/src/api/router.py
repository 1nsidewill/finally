import os
import time
import logging
from datetime import datetime

from fastapi import APIRouter, HTTPException, status, Depends
from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from langchain_community.vectorstores import Chroma
from langchain.prompts import PromptTemplate

from src.config import get_settings
from src.api.schema import PropertyItems, QueryRequest, QueryResponse
from src.api.document_utils import format_docs, load_txt_documents
from src.api.utils import load_prompts
from src.auth.user_service import get_current_user

# 로거 설정
logger = logging.getLogger("api")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# 설정 로드
config = get_settings()
api_router = APIRouter()

# 프롬프트 미리 로드
PROMPTS = load_prompts()

# 벡터스토어 및 리트리버 초기화
logger.info("🔄 벡터스토어 초기화 중...")
DATA_DIR = os.path.join(os.path.dirname(__file__), "../../data")
docs_list = load_txt_documents(DATA_DIR)
vectorstore = Chroma.from_documents(
    documents=docs_list,
    collection_name="rag-chroma",
    embedding=OpenAIEmbeddings(openai_api_key=config.OPENAI_API_KEY, model="text-embedding-3-large"),
)
retriever = vectorstore.as_retriever()
logger.info(f"✅ 벡터스토어 초기화 완료. 문서 {len(docs_list)}개 로드됨")

@api_router.post("/query", response_model=QueryResponse)
async def query(request: QueryRequest, user=Depends(get_current_user)):
    """
    바이크 매물 추천 API 엔드포인트
    
    사용자 질문을 바탕으로 관련성이 높은 매물을 추천하고 순위를 매겨 제공합니다.
    """
    request_id = f"req_{int(time.time())}"
    start_time = time.time()
    logger.info(f"🔍 [{request_id}] 검색 요청 시작 - 사용자: {user}, 쿼리: '{request.question}'")
    
    try:
        # YAML에서 로드한 프롬프트 템플릿 사용
        bike_prompt = PROMPTS.get("bike_recommendation_prompt", "")
        if not bike_prompt:
            logger.error(f"❌ [{request_id}] 프롬프트 템플릿을 찾을 수 없습니다")
            raise ValueError("프롬프트 템플릿을 찾을 수 없습니다")
        
        # 프롬프트 템플릿 설정
        prompt = PromptTemplate.from_template(
            bike_prompt,
            template_format="jinja2"
        )
        
        logger.info(f"📄 [{request_id}] 관련 문서 검색 중...")
        
        # LLM 모델 설정 및 구조화된 출력 구성
        llm_model = ChatOpenAI(
            model_name="gpt-4o",
            api_key=config.OPENAI_API_KEY,
            temperature=0.1
        ).with_structured_output(PropertyItems)

        # 추천 체인 구성
        chain = (
            {
                "input_query": lambda x: x,
                "context": retriever | format_docs
            }
            | prompt
            | llm_model
        )
        
        # LLM에 추천 요청
        logger.info(f"🤖 [{request_id}] LLM에 추천 요청 중...")
        property_items = await chain.ainvoke(request.question)
        
        # 순위에 따라 결과 정렬
        sorted_items = sorted(property_items.items, key=lambda item: item.rank)
        
        # 응답 구성
        response = QueryResponse(
            result=sorted_items,
            message="성공적으로 검색되었습니다",
            success=True
        )
        
        # 처리 완료 로깅
        process_time = time.time() - start_time
        logger.info(f"✅ [{request_id}] 검색 완료: {len(sorted_items)}개 매물 추천, 처리 시간: {process_time:.2f}초")
        
        return response
        
    except Exception as e:
        # 오류 처리 및 로깅
        process_time = time.time() - start_time
        if isinstance(e, ValueError):
            logger.error(f"❌ [{request_id}] 잘못된 요청: {str(e)}, 처리 시간: {process_time:.2f}초")
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
        elif isinstance(e, KeyError):
            logger.error(f"❌ [{request_id}] 처리 오류: {str(e)}, 처리 시간: {process_time:.2f}초")
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))
        else:
            logger.error(f"❌ [{request_id}] 서버 오류: {str(e)}, 처리 시간: {process_time:.2f}초", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                detail="서버 내부 오류: " + str(e)
            )
