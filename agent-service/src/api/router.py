import os
import time
import logging
from datetime import datetime

from fastapi import APIRouter, HTTPException, status, Depends
from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from langchain_qdrant import QdrantVectorStore
from langchain.prompts import PromptTemplate
from qdrant_client import QdrantClient
from qdrant_client.http.models import SearchRequest, VectorParams
from langchain_core.documents import Document

from src.config import get_settings
from src.api.schema import PropertyItems, QueryRequest, QueryResponse
from src.api.document_utils import format_docs, load_txt_documents
from src.api.utils import load_prompts
from src.auth.user_service import get_current_user

# 설정 로드
config = get_settings()
api_router = APIRouter()

# 프롬프트 미리 로드
PROMPTS = load_prompts()

# Qdrant 클라이언트 및 벡터스토어 초기화
logging.info("🔄 Qdrant 벡터스토어 초기화 중...")

# Qdrant 클라이언트 생성
qdrant_client = QdrantClient(
    host=config.QDRANT_HOST,
    port=config.QDRANT_PORT,
    grpc_port=config.QDRANT_GRPC_PORT,
    prefer_grpc=config.QDRANT_PREFER_GRPC
)

logging.info(f"✅ Qdrant 벡터스토어 초기화 완료. 컬렉션: {config.QDRANT_COLLECTION}")

def format_qdrant_docs(docs):
    """Qdrant 검색 결과를 포맷팅하는 함수 - 중요한 필드들 위주"""
    formatted_docs = []
    for doc in docs:
        # metadata에서 중요한 정보 추출
        metadata = doc.metadata
        title = metadata.get('title', '')
        url = metadata.get('url', '')
        img_url = metadata.get('img_url', '')
        price = metadata.get('price', '')
        odo = metadata.get('odo', '')  # 주행거리
        content = doc.page_content
        
        # 추가 정보 (참고용)
        model_name = metadata.get('model_name', '')
        brand = metadata.get('brand', '')
        
        # 가격 포맷팅
        if price and str(price).isdigit():
            formatted_price = f"{int(price):,}원"
        else:
            formatted_price = str(price) if price else "가격미정"
        
        # 주행거리 포맷팅
        formatted_odo = ""
        if odo:
            if str(odo).isdigit():
                formatted_odo = f"주행거리: {int(odo):,}km"
            else:
                formatted_odo = f"주행거리: {odo}"
        
        formatted_doc = f"""
            제목: {title}
            URL: {url if url else "URL 정보 없음"}
            이미지: {img_url if img_url else "이미지 정보 없음"}
            가격: {formatted_price}
            {formatted_odo}
            브랜드/모델: {brand} {model_name}
            내용: {content}
            ---
            """
        formatted_docs.append(formatted_doc.strip())
    
    return "\n\n".join(formatted_docs)

@api_router.post("/query", response_model=QueryResponse)
async def query(request: QueryRequest, user=Depends(get_current_user)):
    """
    바이크 매물 추천 API 엔드포인트
    
    사용자 질문을 바탕으로 관련성이 높은 매물을 추천하고 순위를 매겨 제공합니다.
    """
    request_id = f"req_{int(time.time())}"
    start_time = time.time()
    logging.info(f"🔍 [{request_id}] 검색 요청 시작 - 사용자: {user}, 쿼리: '{request.question}'")
    
    try:
        # YAML에서 로드한 프롬프트 템플릿 사용
        bike_prompt = PROMPTS.get("bike_recommendation_prompt", "")
        if not bike_prompt:
            logging.error(f"❌ [{request_id}] 프롬프트 템플릿을 찾을 수 없습니다")
            raise ValueError("프롬프트 템플릿을 찾을 수 없습니다")
        
        # 프롬프트 템플릿 설정
        prompt = PromptTemplate.from_template(
            bike_prompt,
            template_format="jinja2"
        )
        
        logging.info(f"📄 [{request_id}] Qdrant에서 관련 문서 검색 중...")
        
        # Qdrant 클라이언트로 직접 검색해서 데이터 가져오기
        # 임베딩 생성
        embeddings = OpenAIEmbeddings(openai_api_key=config.OPENAI_API_KEY, model="text-embedding-3-large")
        query_vector = embeddings.embed_query(request.question)
        
        # Qdrant 클라이언트로 직접 검색
        qdrant_search_result = qdrant_client.search(
            collection_name=config.QDRANT_COLLECTION,
            query_vector=query_vector,
            limit=12,
            with_payload=True,  # payload 포함해서 검색
            with_vectors=False
        )
        
        # Qdrant 검색 결과를 Document 객체로 변환
        search_results = []
        for point in qdrant_search_result:
            if point.payload:
                # content를 page_content로 사용
                page_content = point.payload.get('content', '')
                
                # metadata 구성 - 중요한 필드들 위주
                metadata = {
                    'title': point.payload.get('title', ''),
                    'url': point.payload.get('url', ''),  # 현재 데이터에 없지만 곧 추가될 예정
                    'img_url': point.payload.get('img_url', ''),  # 현재 데이터에 없지만 곧 추가될 예정
                    'price': point.payload.get('price', ''),
                    'odo': point.payload.get('odo', ''),  # 곧 추가될 예정
                    'id': point.payload.get('id', ''),
                    'model_name': point.payload.get('model_name', ''),
                    'category': point.payload.get('category', ''),
                    'brand': point.payload.get('brand', ''),
                    'last_modified_at': point.payload.get('last_modified_at', ''),
                    'extra': point.payload.get('extra', {}),
                    'score': point.score
                }
                
                doc = Document(page_content=page_content, metadata=metadata)
                search_results.append(doc)
        
        # 검색 결과 로깅 (디버깅용)
        logging.info(f"🔍 [{request_id}] 변환된 검색결과 수: {len(search_results)}")
        # for i, doc in enumerate(search_results):
        #     logging.info(f"🔍 [{request_id}] 검색결과 {i+1}: {doc.page_content[:100]}... | metadata keys: {list(doc.metadata.keys())}")
        #     logging.info(f"🔍 [{request_id}] 검색결과 {i+1} price: {doc.metadata.get('price')}, model: {doc.metadata.get('model_name')}")
        
        # 검색된 문서들을 포맷팅
        formatted_context = format_qdrant_docs(search_results)
        
        # LLM 모델 설정 및 구조화된 출력 구성
        llm_model = ChatOpenAI(
            model_name="gpt-4.1",
            api_key=config.OPENAI_API_KEY,
            temperature=0.1
        ).with_structured_output(PropertyItems)

        # 추천 체인 구성
        chain = (
            {
                "input_query": lambda x: x,
                "context": lambda x: formatted_context
            }
            | prompt
            | llm_model
        )
        
        # LLM에 추천 요청
        logging.info(f"🤖 [{request_id}] LLM에 추천 요청 중...")
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
        logging.info(f"✅ [{request_id}] 검색 완료: {len(sorted_items)}개 매물 추천, 처리 시간: {process_time:.2f}초")
        
        return response
        
    except Exception as e:
        # 오류 처리 및 로깅
        process_time = time.time() - start_time
        if isinstance(e, ValueError):
            logging.error(f"❌ [{request_id}] 잘못된 요청: {str(e)}, 처리 시간: {process_time:.2f}초")
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
        elif isinstance(e, KeyError):
            logging.error(f"❌ [{request_id}] 처리 오류: {str(e)}, 처리 시간: {process_time:.2f}초")
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))
        else:
            logging.error(f"❌ [{request_id}] 서버 오류: {str(e)}, 처리 시간: {process_time:.2f}초", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                detail="서버 내부 오류: " + str(e)
            )
