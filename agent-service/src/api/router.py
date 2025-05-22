import os

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

config = get_settings()
api_router = APIRouter()

# 벡터스토어 및 리트리버 초기화 (최초 1회만)
DATA_DIR = os.path.join(os.path.dirname(__file__), "../../data")
docs_list = load_txt_documents(DATA_DIR)
vectorstore = Chroma.from_documents(
    documents=docs_list,
    collection_name="rag-chroma",
    embedding=OpenAIEmbeddings(openai_api_key=config.OPENAI_API_KEY, model="text-embedding-3-large"),
)
retriever = vectorstore.as_retriever()

@api_router.post("/query", response_model=QueryResponse)
async def query(request: QueryRequest, user=Depends(get_current_user)):
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
        - content: 매물에 대한 내용. 이미 요약되어 있는 내용이기에 빠짐없이 제공
        - match_summary: 사용자 질문과 매물의 연관성 설명 (추천하는 이유, 캐주얼하게 이모지 사용해도 됨)
        
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
        property_items = await chain.ainvoke(vector_search_query)
        
        # PropertyItems에서 items 필드를 추출하여 QueryResponse에 넣음
        # 응답 결과를 순위(rank)에 따라 정렬
        sorted_items = sorted(property_items.items, key=lambda item: item.rank)
        
        response = QueryResponse(
            result=sorted_items,
            message="성공적으로 검색되었습니다",
            success=True
        )
        
        return response
        
    except Exception as e:
        if isinstance(e, ValueError):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
        elif isinstance(e, KeyError):
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                detail="서버 내부 오류: " + str(e)
            )
