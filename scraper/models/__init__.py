from sqlalchemy import (
    Column, String, Text, BigInteger, SmallInteger, ForeignKey, Numeric, TIMESTAMP,
    UniqueConstraint, JSON, Integer
)
from sqlalchemy.dialects.postgresql import INET
from sqlalchemy.orm import relationship
from core.database import Base


class Provider(Base):
    __tablename__ = "provider"

    uid = Column(BigInteger, primary_key=True, autoincrement=True)
    code = Column(String(50), nullable=False, unique=True)
    provider_url = Column(String(500), nullable=False, unique=True)
    name = Column(String(100), nullable=False)
    desc = Column(String(500))
    created_dt = Column(TIMESTAMP, server_default="CURRENT_TIMESTAMP")
    updated_dt = Column(TIMESTAMP, server_default="CURRENT_TIMESTAMP")

    products = relationship("Product", back_populates="provider")
    images = relationship("Img", back_populates="provider")


class Product(Base):
    __tablename__ = "product"

    uid = Column(BigInteger, primary_key=True, autoincrement=True)
    provider_uid = Column(BigInteger, ForeignKey("provider.uid", ondelete="CASCADE", onupdate="CASCADE"), nullable=False)
    product_code = Column(String(50), nullable=False)
    code = Column(String(50))
    title = Column(String(200), nullable=False)
    content = Column(Text)
    price = Column(Numeric(15, 2))
    location = Column(String(200))
    rmk = Column(JSON)
    desc = Column(Text)
    created_dt = Column(TIMESTAMP, server_default="CURRENT_TIMESTAMP")
    updated_dt = Column(TIMESTAMP, server_default="CURRENT_TIMESTAMP")

    provider = relationship("Provider", back_populates="products")
    images = relationship("Img", back_populates="product")


class Img(Base):
    __tablename__ = "img"

    uid = Column(BigInteger, primary_key=True, autoincrement=True)
    provider_uid = Column(BigInteger, ForeignKey("provider.uid", ondelete="CASCADE", onupdate="CASCADE"), nullable=False)
    product_uid = Column(BigInteger, ForeignKey("product.uid", ondelete="CASCADE", onupdate="CASCADE"), nullable=False)
    code = Column(String(50))
    url = Column(Text, nullable=False)
    created_dt = Column(TIMESTAMP, server_default="CURRENT_TIMESTAMP")
    updated_dt = Column(TIMESTAMP, server_default="CURRENT_TIMESTAMP")

    provider = relationship("Provider", back_populates="images")
    product = relationship("Product", back_populates="images")


class Code(Base):
    __tablename__ = "code"

    uid = Column(BigInteger, primary_key=True, autoincrement=True)
    code = Column(String(50))
    upper_uid = Column(BigInteger, ForeignKey("code.uid", ondelete="SET NULL", onupdate="CASCADE"))
    depth = Column(SmallInteger)
    order = Column(SmallInteger, name="order")
    name = Column(String(100), nullable=False)
    value = Column(Text)
    desc = Column(Text)
    rmk = Column(JSON)
    created_dt = Column(TIMESTAMP, server_default="CURRENT_TIMESTAMP")
    updated_dt = Column(TIMESTAMP, server_default="CURRENT_TIMESTAMP")

    parent = relationship("Code", remote_side=[uid], backref="children")


class Log(Base):
    __tablename__ = "log"

    uid = Column(BigInteger, primary_key=True, autoincrement=True)
    code_uid = Column(BigInteger, ForeignKey("code.uid", ondelete="SET NULL", onupdate="CASCADE"))
    table_name = Column(String(100), nullable=False)
    ip = Column(INET)
    created_dt = Column(TIMESTAMP, server_default="CURRENT_TIMESTAMP")
    updated_dt = Column(TIMESTAMP, server_default="CURRENT_TIMESTAMP")
