from sqlalchemy import Column, String, Text, BigInteger, SmallInteger, TIMESTAMP, ForeignKey
from sqlalchemy.orm import relationship
from core.database import Base

class File(Base):
    __tablename__ = "file"

    uid = Column(BigInteger, primary_key=True, autoincrement=True, comment="자동증가 기본키")
    provider_uid = Column(BigInteger, ForeignKey("provider.uid", ondelete="SET NULL", onupdate="CASCADE"), comment="provider 테이블 FK")
    category_uid = Column(BigInteger, ForeignKey("category.uid", ondelete="SET NULL", onupdate="CASCADE"), comment="category 테이블 FK")
    product_uid = Column(BigInteger, ForeignKey("product.uid", ondelete="SET NULL", onupdate="CASCADE"), comment="product 테이블 FK")
    url = Column(Text, comment="파일(URL) 주소")
    path = Column(String(300), comment="서버 저장 경로(상대/절대)")
    count = Column(SmallInteger, default=0, comment="파일 수량")
    created_dt = Column(TIMESTAMP, server_default="CURRENT_TIMESTAMP", comment="생성일시")
    updated_dt = Column(TIMESTAMP, server_default="CURRENT_TIMESTAMP", comment="수정일시")

    provider = relationship("Provider", back_populates="file")
    category = relationship("Category", back_populates="file")
    product = relationship("Product", back_populates="file")
