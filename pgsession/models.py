from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, BigInteger, Integer, String, Boolean, DateTime, Float
import pgsession.constants as c

Base = declarative_base()

class DataRows(Base):
    __tablename__ = c.TABLE_NAME
    id = Column(BigInteger, primary_key=True)
    by = Column(String)
    score = Column(BigInteger)
    time = Column(BigInteger)
    time_ts = Column(String)
    title = Column(String)
    url = Column(String)
    text = Column(String)
    deleted = Column(BigInteger)
    dead = Column(Boolean)
    descendants = Column(Float(precision=53))
    author = Column(String)
    date = Column(DateTime(timezone=True))
    quarter = Column(BigInteger)
    year = Column(BigInteger)
    month = Column(BigInteger)
    day = Column(BigInteger)
    text_len = Column(BigInteger)