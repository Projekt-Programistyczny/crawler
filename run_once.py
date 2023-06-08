from src.database import add_links, select_links,select_not_active_links, deactive_link, reactive_link
from src.crawler import Olx_Crawler, OtoDom_Crawler, OFFER, ESTATE
from typing import List, Dict
from time import time
import yaml
import logging
import os

logging.basicConfig(filename='logfile.txt', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')



absolute_path = os.path.dirname(__file__)
cities_path = os.path.join(absolute_path, 'cities.yaml')

with open(cities_path, 'r') as f:
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


def get_list_of_expired_links(city: str, type_of_estate: str, type_of_offer: str) -> List[Dict[str, str]]:
    links = select_not_active_links(city, type_of_estate, type_of_offer)
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

    # get active links
    old = get_list_of_existing_links(city, type_of_estate.value, type_of_offer.value)
    new = []

    for link in unique_links:
        new.append(build_dict_for_db(link,
                                     city,
                                     type_of_offer.value,
                                     type_of_estate.value,
                                     is_active=True))

    # chceck only url
    old_url = [link['url'] for link in old]
    if 'https://www.otodom.pl/pl/oferta/domy-w-stanie-deweloperskim-oddane-do-uzytku-ID4lDwO' in old_url:
        print("jest w starych")
    new_url = [link['url'] for link in new]
    if 'https://www.otodom.pl/pl/oferta/domy-w-stanie-deweloperskim-oddane-do-uzytku-ID4lDwO' in new_url:
        print("jest w nowych")

    # update not existing links
    new_links_url = list(filter(lambda x: x not in old_url, new_url))
    new_links = [link for link in new if link['url'] in new_links_url]

    # Add new links to db
    add_links(new_links)

    # Chek if link need to be reactive
    expired_links = get_list_of_expired_links(city, type_of_estate.value, type_of_offer.value)
    expired_url = [link['url'] for link in expired_links]
    reactive_links_url = list(filter(lambda x: x in expired_url, new_url))
    reactive_links = [link for link in expired_links if link['url'] in reactive_links_url]
    for link in reactive_links:
        print(link)
        reactive_link(link["url"])

    # update not_active links
    not_active_url = list(filter(lambda x: x not in new_url, old_url))
    not_active = [link for link in old if link['url'] in not_active_url]

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

