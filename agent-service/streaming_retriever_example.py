from typing import Annotated, Sequence, TypedDict
from langgraph.graph import Graph, StateGraph
from langchain_openai import ChatOpenAI
from langchain_community.vectorstores import FAISS
from langchain_openai import OpenAIEmbeddings
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_core.documents import Document
from langchain_core.messages import HumanMessage, AIMessage
import os
from dotenv import load_dotenv

# 환경 변수 로드\\
# 상태 정의
class AgentState(TypedDict):
    messages: Annotated[Sequence[HumanMessage | AIMessage], "대화 메시지"]
    context: Annotated[list[str], "검색된 컨텍스트"]

# 리트리버 설정
def setup_retriever():
    # 예시 문서
    texts = [
        "LangGraph는 LangChain의 워크플로우를 위한 프레임워크입니다.",
        "스트리밍은 실시간으로 응답을 생성하는 방식입니다.",
        "Context7은 컨텍스트 관리를 위한 도구입니다."
    ]
    
    # 문서 분할
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=100,
        chunk_overlap=20
    )
    docs = [Document(page_content=t) for t in texts]
    splits = text_splitter.split_documents(docs)
    
    # 벡터 저장소 생성
    embeddings = OpenAIEmbeddings()
    vectorstore = FAISS.from_documents(splits, embeddings)
    
    return vectorstore.as_retriever()

# 노드 함수들
def retrieve(state: AgentState) -> AgentState:
    """컨텍스트 검색"""
    retriever = setup_retriever()
    query = state["messages"][-1].content
    docs = retriever.get_relevant_documents(query)
    return {"context": [doc.page_content for doc in docs]}

def generate_response(state: AgentState) -> AgentState:
    """응답 생성"""
    llm = ChatOpenAI(
        model="gpt-3.5-turbo",
        streaming=True
    )
    
    # 컨텍스트와 메시지 결합
    context = "\n".join(state["context"])
    messages = state["messages"] + [
        HumanMessage(content=f"Context: {context}\n\nQuestion: {state['messages'][-1].content}")
    ]
    
    # 스트리밍 응답 생성
    response = llm.stream(messages)
    full_response = ""
    
    # 스트리밍 응답 처리
    for chunk in response:
        if chunk.content:
            full_response += chunk.content
            print(chunk.content, end="", flush=True)
    
    print()  # 줄바꿈
    return {"messages": state["messages"] + [AIMessage(content=full_response)]}

# 그래프 구성
def create_graph() -> Graph:
    workflow = StateGraph(AgentState)
    
    # 노드 추가
    workflow.add_node("retrieve", retrieve)
    workflow.add_node("generate", generate_response)
    
    # 엣지 추가
    workflow.add_edge("retrieve", "generate")
    
    # 시작 노드 설정
    workflow.set_entry_point("retrieve")
    
    return workflow.compile()

# 실행 예제
if __name__ == "__main__":
    graph = create_graph()
    
    # 초기 상태
    initial_state = {
        "messages": [HumanMessage(content="LangGraph에 대해 설명해줘")],
        "context": []
    }
    
    # 그래프 실행
    for output in graph.stream(initial_state):
        if "generate" in output:
            print("\n최종 응답:", output["generate"]["messages"][-1].content) 