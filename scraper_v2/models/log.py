from sqlalchemy import Column, String, Text, BigInteger, SmallInteger, TIMESTAMP
from sqlalchemy.dialects.postgresql import INET
from core.database import Base

class Log(Base):
    __tablename__ = "log"

    uid = Column(BigInteger, primary_key=True, autoincrement=True, comment="자동증가 기본키")
    table_name = Column(String(100), nullable=False, comment="대상 테이블명")
    table_uid = Column(BigInteger, nullable=False, comment="대상 row uid")
    ip = Column(INET, comment="요청 IP")
    action = Column(String(150), nullable=False, comment="액션명(로그타입)")
    status = Column(String(50), nullable=False, comment="상태값")
    desc = Column(Text, comment="비고/상세내용")
    created_dt = Column(TIMESTAMP, server_default="CURRENT_TIMESTAMP", comment="생성일시")
    updated_dt = Column(TIMESTAMP, server_default="CURRENT_TIMESTAMP", comment="수정일시")
