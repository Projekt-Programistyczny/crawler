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


def add_links(list_of_links) -> None:
    links_to_save = []
    with SessionLocal() as db:
        for link in list_of_links:
            links_to_save.append(ModelLink(url=link["url"],
                                           city_name=link["city_name"],
                                           type_of_estate=link["type_of_estate"],
                                           type_of_offer=link["type_of_offer"],
                                           used=False,
                                           is_active=link["is_active"]
                                           )
                                 )
        db.add_all(links_to_save)
        db.commit()


def select_links():
    with SessionLocal() as db:
        links_details = db.query(ModelLink).all()
        return links_details


def select_links(city: str, estate: str, offer: str):
    with SessionLocal() as db:
        links_details = db.query(ModelLink).filter_by(city_name=city,
                                                      type_of_estate=estate,
                                                      type_of_offer=offer).all()
        return links_details
    
def select_not_active_links(city: str, estate: str, offer: str):
    with SessionLocal() as db:
        links_details = db.query(ModelLink).filter_by(city_name=city,
                                                      type_of_estate=estate,
                                                      type_of_offer=offer,
                                                      is_active=False).all()
        return links_details


def select_cities():
    with SessionLocal() as db:
        cities_details = db.query(ModelCity).all()
        return cities_details

def reactive_link(url):
    with SessionLocal() as db:
        row = db.query(ModelLink).filter_by(url=url).first()
        # row.used = False
        row.is_active = True
        db.commit()

def deactive_link(url):
    with SessionLocal() as db:
        row = db.query(ModelLink).filter_by(url=url).first()
        row.is_active = False
        db.commit()
