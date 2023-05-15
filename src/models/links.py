from sqlalchemy import Column, DateTime, Integer, String, Boolean, JSON
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Link(Base):
    __table_args__ = {"extend_existing": True}
    __tablename__ = "links"
    id = Column(Integer, primary_key=True)
    url = Column(String)
    city_name = Column(String)
    type_of_estate = Column(String)
    type_of_offer = Column(String)
    used = Column(Boolean)
    is_active = Column(Boolean)
