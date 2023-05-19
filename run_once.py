from src.database import add_links, select_links, deactive_link
from src.crawler import Olx_Crawler, OtoDom_Crawler, OFFER, ESTATE
from typing import List, Dict
from time import time
import yaml
import logging

logging.basicConfig(filename='logfile.txt', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

with open('cities.yaml', 'r') as f:
    cities_yaml = yaml.safe_load(f)

CRAWLER_INTERVAL = 1 * 60 * 60 # 1 hour
CITY_TO_EXPLORE = cities_yaml["region"]["cities"]


def build_dict_for_db(url: str, city_name: str, type_of_estate: str, type_of_offer: str, is_active: bool=True) -> Dict[str, str]:
    return {"url": url,
            "city_name": city_name,
            "type_of_estate": type_of_offer,
            "type_of_offer": type_of_estate,
            "is_active": is_active}


def get_list_of_existing_links(city: str, type_of_estate: str, type_of_offer: str) -> List[Dict[str, str]]:
    links = select_links(city, type_of_estate, type_of_offer)
    out = []
    for link in links:
        out.append(build_dict_for_db(link.url,
                                     link.city_name,
                                     link.type_of_offer,
                                     link.type_of_estate,
                                     link.is_active))
    return out


def run_crawler(city: str, type_of_offer: OFFER, type_of_estate: ESTATE) -> None:
    crawler_olx = Olx_Crawler(type_of_offer = type_of_offer, type_of_estate=type_of_estate, city=city)
    crawler_otoDom = OtoDom_Crawler(type_of_offer = type_of_offer, type_of_estate=type_of_estate, city=city)

    logging.info(f"[{city}-{type_of_offer.value}-{type_of_estate.value}] Start search for Olx links...")

    try:
        links_olx = crawler_olx.run()
    except:
        links_olx = []

    print()
    logging.info(f"[{city}-{type_of_offer.value}-{type_of_estate.value}] Start search for OtoDom links...")

    try:
        links_otoDom = crawler_otoDom.run()
    except:
        links_otoDom = []
    links = links_olx + links_otoDom

    unique_links = list(set(links))

    old = get_list_of_existing_links(city, type_of_estate.value, type_of_offer.value)
    new = []

    for link in unique_links:
        new.append(build_dict_for_db(link,
                                     city,
                                     type_of_offer.value,
                                     type_of_estate.value,
                                     is_active=True))
    new = list(filter(lambda x: x not in old, new))
    add_links(new)

    # update not_active links
    not_active = list(filter(lambda x: x not in new, old))
    for link in not_active:
        deactive_link(link["url"])

    print()
    logging.info(f"Added {len(new)} new (unique) rows")


if __name__ == "__main__":
    for city in CITY_TO_EXPLORE:
        run_crawler(city, OFFER.RENT, ESTATE.APARTMENT)
        run_crawler(city, OFFER.SALE, ESTATE.APARTMENT)
        run_crawler(city, OFFER.RENT, ESTATE.HOUSE)
        run_crawler(city, OFFER.SALE, ESTATE.HOUSE)

