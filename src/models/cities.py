from sqlalchemy import Column, DateTime, Integer, String, Boolean, JSON
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class City(Base):
    __table_args__ = {"extend_existing": True}
    __tablename__ = "cities"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    voivodeship = Column(String)
    amount_of_citizens = Column(Integer)
