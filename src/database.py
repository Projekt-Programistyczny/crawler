import os

from dotenv import load_dotenv

from sqlalchemy import create_engine, desc 
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from src.models.cities import City as ModelCity
from src.models.links import Link as ModelLink

load_dotenv('.env')

SQLALCHEMY_DATABASE_URL = os.environ["DATABASE_URI"]
engine = create_engine(
    SQLALCHEMY_DATABASE_URL
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def add_link(url, city_name, type_of_estate, type_of_offer) -> None:
    with SessionLocal() as db:
        db.query(ModelLink).add(
            {
                ModelLink.url: url,
                ModelLink.city_name: city_name,
                ModelLink.type_of_estate: type_of_estate,
                ModelLink.type_of_offer: type_of_offer,
                ModelLink.used: False
            }
        )
        db.commit()


def select_cities():
    with SessionLocal() as db:
        cities_details = db.query(ModelCity).all()
        return cities_details
