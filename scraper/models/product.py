from sqlalchemy import Column, String, Text, BigInteger, SmallInteger, Integer, TIMESTAMP, ForeignKey, Numeric, JSON
from sqlalchemy.orm import relationship
from core.database import Base

class Product(Base):
    __tablename__ = "product"

    uid = Column(BigInteger, primary_key=True, autoincrement=True, comment="자동증가 기본키")
    provider_uid = Column(BigInteger, ForeignKey("provider.uid", ondelete="CASCADE", onupdate="CASCADE"), nullable=False, comment="provider 테이블 FK")
    pid = Column(String(60), nullable=False, comment="상품 고유 번호")
    status = Column(SmallInteger, nullable=False, comment="1:판매중, 2:예약중, 3:판매완료, 9:삭제")
    title = Column(String(200), nullable=False, comment="제목")
    brand = Column(String(30), comment="브랜드 명")
    content = Column(Text, comment="내용")
    price = Column(Numeric(15, 2), comment="가격")
    location = Column(String(200), comment="위치")
    category = Column(String(100), comment="카테고리명")
    color = Column(String(30), comment="색상")
    odo = Column(Integer, comment="주행거리")
    year = Column(Integer, comment="연식")
    rmk = Column(JSON, comment="비고/추가정보(JSON)")
    desc = Column(Text, comment="비고/상세내용")
    created_dt = Column(TIMESTAMP, server_default="CURRENT_TIMESTAMP", comment="생성일시")
    updated_dt = Column(TIMESTAMP, server_default="CURRENT_TIMESTAMP", comment="수정일시")

    provider = relationship("Provider", back_populates="products")
    file = relationship("File", back_populates="product")
