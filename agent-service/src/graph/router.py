import os

from fastapi import APIRouter
from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from langchain_community.vectorstores import Chroma
from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain.schema import Document

from src.config import get_settings

config = get_settings()
graph_router = APIRouter()

def load_txt_documents(data_dir: str) -> list:
    """data_dir 하위의 모든 .txt 파일을 Document 객체로 반환"""
    docs = []
    for file_name in os.listdir(data_dir):
        if file_name.endswith(".txt"):
            file_path = os.path.join(data_dir, file_name)
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
                docs.append(Document(page_content=content, metadata={"file_name": file_name}))
    return docs

def format_docs(docs):
    return "\n\n".join(
        f"[{doc.metadata.get('file_name', 'unknown')}] {doc.page_content}"
        for doc in docs
    )

# 벡터스토어 및 리트리버 초기화 (최초 1회만)
DATA_DIR = os.path.join(os.path.dirname(__file__), "../../data")
docs_list = load_txt_documents(DATA_DIR)
vectorstore = Chroma.from_documents(
    documents=docs_list,
    collection_name="rag-chroma",
    embedding=OpenAIEmbeddings(openai_api_key=config.OPENAI_API_KEY, model="text-embedding-3-large"),
)
retriever = vectorstore.as_retriever()

@graph_router.get("/query")
async def query(input_query: str):
    system_instructions = """
    너는 바이크 중고매물을 사용자에게 추천하는 사람이야. 
    내가 제공하는 매물 리스트 중에 제일 사용자의 질문과 유사한 매물을 골라 추천해줘. 
    여러개가 사용자의 질문에 잘맞으면 순위를 매겨 다중 매물을 추천해줘도돼.
    답변엔 file_name이 포함되어야해. 예: '1.txt' """

    # # Setup retrievers
    # milvus_vector_store = Milvus(
    #     OpenAIEmbeddings(openai_api_key=config.OPENAI_API_KEY, model="text-embedding-3-large"),
    #     connection_args={"host": config.MILVUS_URL.split(":")[0], "port": config.MILVUS_URL.split(":")[1]},
    #     collection_name=config.MILVUS_COLLECTION_NAME,
    # )
    
    # milvus_retriever = milvus_vector_store.as_retriever(search_type="similarity", search_kwargs={"k": 10})
    
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

    # LLM 모델 생성
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
    response = chain.invoke(vector_search_query)
    return response
