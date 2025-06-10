from sqlalchemy import Column, String, Text, BigInteger, SmallInteger, TIMESTAMP, JSON, ForeignKey
from sqlalchemy.orm import relationship
from core.database import Base

class Code(Base):
    __tablename__ = "code"

    uid = Column(BigInteger, primary_key=True, autoincrement=True, comment="자동증가 기본키")
    code = Column(String(50), comment="코드값")
    upper_uid = Column(BigInteger, ForeignKey("code.uid", ondelete="SET NULL", onupdate="CASCADE"), comment="상위 코드 uid")
    depth = Column(SmallInteger, comment="코드 깊이")
    order = Column(SmallInteger, name="order", comment="정렬 순서")
    name = Column(String(100), nullable=False, comment="코드 이름")
    value = Column(Text, comment="코드 값")
    desc = Column(Text, comment="설명")
    rmk = Column(JSON, comment="비고/추가정보(JSON)")
    created_dt = Column(TIMESTAMP, server_default="CURRENT_TIMESTAMP", comment="생성일시")
    updated_dt = Column(TIMESTAMP, server_default="CURRENT_TIMESTAMP", comment="수정일시")

    parent = relationship("Code", remote_side=[uid], backref="children")
