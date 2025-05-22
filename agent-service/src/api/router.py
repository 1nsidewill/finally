import os
import time
import logging
from datetime import datetime

from fastapi import APIRouter, HTTPException, status, Depends, Request
from fastapi.responses import JSONResponse
from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from langchain_community.vectorstores import Chroma
from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain.schema import Document

from src.config import get_settings
from src.auth.jwt_utils import create_access_token, verify_access_token
from src.api.schema import *
from src.api.document_utils import format_docs, load_txt_documents
from src.auth.user_service import get_current_user

# 로거 설정
logger = logging.getLogger("api")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

config = get_settings()
api_router = APIRouter()

# 벡터스토어 및 리트리버 초기화 (최초 1회만)
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
    request_id = f"req_{int(time.time())}"
    start_time = time.time()
    logger.info(f"🔍 [{request_id}] 검색 요청 시작 - 사용자: {user}, 쿼리: '{request.question}'")
    
    try:
        input_query = request.question
        system_instructions = """
        너는 바이크 중고매물을 사용자에게 추천하는 전문가입니다.
        내가 제공하는 매물 리스트 중에서 사용자의 질문과 가장 유사한 매물들을 선별하여 추천해야 합니다.
        
        반드시 구조화된 형식으로 응답해야 하며, 각 매물에 대한 정보를 아래와 같이 작성하세요:
        - rank: 추천 순위 (1이 가장 높은 순위, 2, 3, ... 순으로 낮아짐)
        - title: 매물의 제목 (예: 야마하 R3 2018)
        - url: 매물 상세 페이지 URL (예: https://example.com/listings/ID)
        - img_url: 매물 이미지 URL (예: https://example.com/images/ID.jpg)
        - price: 매물 가격 (숫자로만 표기, 예: 4600000)
        - content: 매물의 내용. 이미 요약되어 있기 때문에 그대로 제공
        - match_summary: 사용자 질문과 매물의 연관성 설명 (추천하는 이유)
        
        사용자의 질문에 맞는 매물을 선별하여 추천하고, 반드시 사용자 질문과의 관련성에 따라 순위를 매겨서 제공하세요.
        순위 1이 가장 사용자 요구사항에 맞는 매물이어야 합니다.
        """
        
        # Prompt 템플릿 정의
        prompt_template = f"""
            # 시스템 지시사항
            {system_instructions}
            
            # 사용자가 찾는 매물 상세 내용
            {input_query}

            # 중고매물을 추천하기 위해 선별한 유사한 매물(내용)
            {{context}}
        """
        prompt = PromptTemplate.from_template(prompt_template)
        
        logger.info(f"📄 [{request_id}] 관련 문서 검색 중...")
        
        # with_structured_output으로 PropertyItems 형식 지정
        llm_model = ChatOpenAI(
            model_name="gpt-4o",
            api_key=config.OPENAI_API_KEY,
            temperature=0.1  # 일관된 출력을 위해 낮은 temperature 설정
        ).with_structured_output(PropertyItems)

        chain = (
            {
                "context": retriever | format_docs
            }
            | prompt
            | llm_model
        )
        
        vector_search_query = input_query
        logger.info(f"🤖 [{request_id}] LLM에 추천 요청 중...")
        property_items = await chain.ainvoke(vector_search_query)
        
        # PropertyItems에서 items 필드를 추출하여 QueryResponse에 넣음
        # 응답 결과를 순위(rank)에 따라 정렬
        sorted_items = sorted(property_items.items, key=lambda item: item.rank)
        
        response = QueryResponse(
            result=sorted_items,
            message="성공적으로 검색되었습니다",
            success=True
        )
        
        process_time = time.time() - start_time
        logger.info(f"✅ [{request_id}] 검색 완료: {len(sorted_items)}개 매물 추천, 처리 시간: {process_time:.2f}초")
        
        return response
        
    except Exception as e:
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
