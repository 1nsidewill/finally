import os
from langchain.schema import Document

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