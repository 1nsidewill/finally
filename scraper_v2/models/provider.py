from sqlalchemy import Column, String, Text, BigInteger, TIMESTAMP
from sqlalchemy.orm import relationship
from core.database import Base

class Provider(Base):
    __tablename__ = "provider"

    uid = Column(BigInteger, primary_key=True, autoincrement=True, comment="자동증가 기본키")
    code = Column(String(50), nullable=False, unique=True, comment="프로바이더 코드(유니크)")
    url_main = Column(String(500), comment="메인 URL")
    url_api = Column(String(500), comment="API URL")
    url_logo = Column(Text, comment="로고 URL")
    name = Column(String(100), nullable=False, comment="프로바이더 한글명")
    name_english = Column(String(100), nullable=False, comment="프로바이더 영문명")
    desc = Column(String(500), comment="설명")
    created_dt = Column(TIMESTAMP, server_default="CURRENT_TIMESTAMP", comment="생성일시")
    updated_dt = Column(TIMESTAMP, server_default="CURRENT_TIMESTAMP", comment="수정일시")

    products = relationship("Product", back_populates="provider")
    categories = relationship("Category", back_populates="provider")
    file = relationship("File", back_populates="provider")
