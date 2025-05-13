import os
from typing import TypedDict, List
from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.memory import InMemorySaver
from src.config import get_settings
from langchain_community.vectorstores import Chroma
from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from langchain.schema import Document
from langchain import hub
import asyncio
import logging

# Setup logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 공식 문서 스타일의 State 정의
class GraphState(TypedDict):
    question: str
    documents: List[Document]
    answer: str

# config 및 chroma retriever 재사용
config = get_settings()

# 문서 로드 함수 (router.py와 동일)
def load_txt_documents(data_dir: str) -> list:
    logger.info("📄 Loading documents from %s", data_dir)
    docs = []
    for file_name in os.listdir(data_dir):
        if file_name.endswith(".txt"):
            file_path = os.path.join(data_dir, file_name)
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
                docs.append(Document(page_content=content, metadata={"file_name": file_name}))
    logger.info("✅ Loaded %d documents", len(docs))
    return docs

# 벡터스토어 및 리트리버 초기화 (최초 1회만)
DATA_DIR = os.path.join(os.path.dirname(__file__), "../../data")
logger.info("🔍 Initializing vectorstore...")
docs_list = load_txt_documents(DATA_DIR)
vectorstore = Chroma.from_documents(
    documents=docs_list,
    collection_name="rag-chroma",
    embedding=OpenAIEmbeddings(openai_api_key=config.OPENAI_API_KEY, model="text-embedding-3-large"),
)
retriever = vectorstore.as_retriever()
logger.info("🧠 Vectorstore initialized with %d documents", len(docs_list))

# 1. async retrieve node
type StateType = GraphState  # 타입 힌트용 별칭
async def retrieve_node(state: StateType) -> StateType:
    logger.info("🔎 [Node: retrieve] Entered with question: '%s'", state["question"])
    question = state["question"]
    documents = await retriever.aget_relevant_documents(question)
    logger.info("📚 [Node: retrieve] Retrieved %d documents", len(documents))
    return {"documents": documents, "question": question, "answer": state.get("answer", "")}

# 2. async answer node (OpenAI LLM 활용)
async def answer_node(state: StateType) -> StateType:
    logger.info("🤖 [Node: answer] Generating answer for question: '%s'", state["question"])
    question = state["question"]
    documents = state["documents"]
    context = "\n".join([doc.page_content for doc in documents])

    prompt = hub.pull("rlm/rag-prompt")
    prompt += "사용자의 질문이 한글로 되어있다면 무조건 한글로 답변해."

    llm = ChatOpenAI(model_name="gpt-4o", api_key=config.OPENAI_API_KEY)
    response = await llm.ainvoke(prompt.format(context=context, question=question))
    logger.info("✅ [Node: answer] Answer generated (length: %d)", len(response.content))
    return {"answer": response.content, "question": question, "documents": documents}

# State 구조를 명확히 지정 (TypedDict 기반)
graph_builder = (
    StateGraph(GraphState)
    .add_node("retrieve", retrieve_node)
    .add_node("answer", answer_node)
    .add_edge(START, "retrieve")
    .add_edge("retrieve", "answer")
    .add_edge("answer", END)
)

memory = InMemorySaver()
graph = graph_builder.compile(checkpointer=memory)

# 비동기 실행 예시
def print_state(state: GraphState):
    logger.info("📝 [State Update] %s", {k: (v if k != 'documents' else f'{len(v)} docs') for k, v in state.items()})
    print("\n[STATE UPDATE]")
    for k, v in state.items():
        if k == "documents":
            print(f"{k}: {[d.page_content[:30]+'...' for d in v]}")
        else:
            print(f"{k}: {v}")

async def run_graph_async():
    inputs: GraphState = {"question": "에이전트 메모리의 종류는?", "documents": [], "answer": ""}
    async for output in graph.astream(inputs, stream_mode="updates"):
        print_state(output)

# if __name__ == "__main__":
#     logger.info("🚀 Starting async graph run...")
#     asyncio.run(run_graph_async())
