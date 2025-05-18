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
from src.api.models import QueryRequest, QueryResponse
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
        너는 바이크 중고매물을 사용자에게 추천하는 사람이야. 
        내가 제공하는 매물 리스트 중에 제일 사용자의 질문과 유사한 매물을 골라 추천해줘. 
        여러개가 사용자의 질문에 잘맞으면 순위를 매겨 다중 매물을 추천해줘도돼.
        답변엔 file_name이 포함되어야해. 예: '1.txt' """
        # Prompt 템플릿 정의
        prompt_template = f"""
            # 시스템 지시사항 \n
            {system_instructions} \n
            # 사용자가 찾는 매물 상세 내용
            {input_query}

            # 중고매물을 추천하기 위해 선별한 유사한 매물(내용) \n
            {{context}} \n
        """
        prompt = PromptTemplate.from_template(prompt_template)
        llm_model = ChatOpenAI(
            model_name="gpt-4o",
            api_key=config.OPENAI_API_KEY,
        )
        chain = (
            {
                "context": retriever | format_docs
            }
            | prompt
            | llm_model
            | StrOutputParser()
        )
        vector_search_query = input_query
        response = await chain.ainvoke(vector_search_query)
        return JSONResponse(status_code=200, content=QueryResponse(result=response).dict())
    except Exception as e:
        if isinstance(e, ValueError):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
        elif isinstance(e, KeyError):
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))
        else:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="서버 내부 오류: " + str(e))
