from sqlalchemy import Column, String, BigInteger, SmallInteger, TIMESTAMP, ForeignKey, Integer
from sqlalchemy.orm import relationship
from core.database import Base

class Category(Base):
    __tablename__ = "category"

    uid = Column(BigInteger, primary_key=True, autoincrement=True, comment="자동증가 기본키")
    provider_uid = Column(BigInteger, ForeignKey("provider.uid", ondelete="SET NULL", onupdate="CASCADE"), comment="provider 테이블 FK")
    title = Column(String(200), nullable=False, comment="카테고리명")
    id = Column(String(100), nullable=False, comment="카테고리 ID(코드)")
    depth = Column(SmallInteger, comment="카테고리 깊이")
    order = Column(Integer, name="order", comment="정렬 순서")
    created_dt = Column(TIMESTAMP, server_default="CURRENT_TIMESTAMP", comment="생성일시")
    updated_dt = Column(TIMESTAMP, server_default="CURRENT_TIMESTAMP", comment="수정일시")

    provider = relationship("Provider", back_populates="categories")
    file = relationship("File", back_populates="category")
