from requests.adapters import HTTPAdapter
from requests.sessions import Session
from requests import Response
from urllib3.util import Retry
from abc import ABC, abstractmethod
from typing import List, Union
from tenacity import retry
from tenacity.wait import wait_exponential
from enum import Enum
import json
import bs4
import re


"""
Author: Kamil Wieczorek
Contact: vieczorkamil@gmail.com
Version: 0.2
Date: 24-04-2023
"""

# Allow only this type of offer
class OFFER(Enum):
    RENT = "wynajem"
    SALE = "sprzedaz"

# Allow only this type of estate 
class ESTATE(Enum):
    APARTMENT = "mieszkanie"
    HOUSE = "dom"

class Crawler_Base(ABC):
    def __init__(self, city: str,
                       type_of_offer: OFFER,
                       type_of_estate: ESTATE,
                       tag_name: str,
                       class_name: str,
                       session_retry_connect: int=15,
                       session_retry_read: int=15,
                       session_retry_redirect: int=15) -> None:
        
        self.city = city
        self.type_of_offer = type_of_offer
        self.type_of_estate = type_of_estate
        self.tag_name = tag_name
        self.class_name = class_name
        self.session_retry_connect = session_retry_connect
        self.session_retry_read = session_retry_read
        self.session_retry_redirect = session_retry_redirect
        self.page_limit = 150

        self.session = self._init_session()


    def set_page_limit(self, limit: int) -> None:
        self.page_limit = limit


    def run(self) -> List[str]:
        list_of_offers = []
        iterator = 0
        page = 1
        # scrapp first page to get number of offers
        soup = self._get_page(page)
        num_of_offers = self._get_number_of_offers(soup)
        links = self._get_link_section(soup)
        hrefs = self._get_links(links)

        # DEBUG:
        page_type = self._get_url(1, type_of_offer=self.type_of_offer, 
                                     type_of_estate=self.type_of_estate, 
                                     city=self.city)
        if "olx" in page_type:
            page_type = "OLX"
        elif "otodom" in page_type:
            page_type = "OtoDom"

        for href in hrefs:
            if href not in list_of_offers:
                list_of_offers.append(href)
                iterator += 1

            if iterator >= num_of_offers:
                break

        # iterate across all pages
        while(iterator < num_of_offers):
            page += 1
            soup = self._get_page(page)

            links = self._get_link_section(soup)
            hrefs = self._get_links(links)
            print("                                 ", end='\r')
            print(f"Saved {iterator} links", end="\r")

            for href in hrefs:
                if href not in list_of_offers:
                    list_of_offers.append(href)
                    iterator += 1

                if iterator >= num_of_offers:
                    break

            # limit of crawled pages
            if page > self.page_limit:
                break

        print("                                 ", end='\r')
        print(f"Saved {iterator} links", end="\r")
        return list_of_offers


    @abstractmethod
    def _get_url(self, page_no: int, type_of_offer: str, type_of_estate: str, city: str) -> str:
        pass


    @abstractmethod
    def _get_link_section(self, soup: bs4.BeautifulSoup) -> Union[bs4.element.ResultSet, List[str]]:
        pass


    @abstractmethod
    def _get_links(self, link_section: Union[bs4.element.ResultSet, List[str]]) -> List[str]:
        pass


    def _get_number_of_offers(self, soup: bs4.BeautifulSoup) -> int:
        h = soup.find_all(name=self.tag_name, class_=self.class_name)[0].text
        return int(re.findall(r'\d+', h)[0])
    

    def _init_session(self) -> Session:
        session = Session()
        retries = Retry(connect=self.session_retry_connect,
                        read=self.session_retry_read,
                        redirect=self.session_retry_redirect)

        session.mount("http://", HTTPAdapter(max_retries=retries))
        session.mount("https://", HTTPAdapter(max_retries=retries))

        return session
    

    @retry(wait=wait_exponential(multiplier=1, min=2, max=5))
    def _get_page(self, page_no: int) -> bs4.BeautifulSoup:
        constructed_url = self._get_url(page_no=page_no, 
                                        type_of_offer=self.type_of_offer, 
                                        type_of_estate=self.type_of_estate, 
                                        city=self.city)

        with self.session as s:
            page_source: Response = s.get(url=constructed_url)
            soup = bs4.BeautifulSoup(page_source.text, "html.parser")

        return soup


class OtoDom_Crawler(Crawler_Base):

    def __init__(self, city: str, type_of_offer: OFFER, type_of_estate: ESTATE,
                 session_retry_connect: int = 15, 
                 session_retry_read: int = 15, 
                 session_retry_redirect: int = 15) -> None:
        super().__init__(city, type_of_offer, type_of_estate, 
                         tag_name="span", class_name="css-19fwpg e17mqyjp2",
                         session_retry_connect=session_retry_connect, 
                         session_retry_read=session_retry_read, 
                         session_retry_redirect=session_retry_redirect)

    def _get_url(self, page_no: int, type_of_offer: OFFER, type_of_estate: ESTATE, city: str) -> str:
        if type_of_offer == OFFER.RENT:
            str_offer = "wynajem"
        elif type_of_offer == OFFER.SALE:
            str_offer = "sprzedaz"

        if type_of_estate == ESTATE.APARTMENT:
            str_estate = "mieszkanie"
        elif type_of_estate == ESTATE.HOUSE:
            str_estate = "dom"

        if city == "cala-polska":
            #TODO:
            pass

        return f"https://www.otodom.pl/pl/oferty/{str_offer}/{str_estate}/{city}?distanceRadius=0&page={int(page_no)}&limit=36&by=DEFAULT&direction=DESC&viewType=listing"


    def _get_link_section(self, soup: bs4.BeautifulSoup) -> Union[bs4.element.ResultSet, List[str]]:          
        links = soup.find(id="__NEXT_DATA__").text
        links = json.loads(links)
        return links["props"]["pageProps"]["data"]["searchAds"]["items"]


    def _get_links(self, link_section: Union[bs4.element.ResultSet, List[str]]) -> List[str]:
        hrefs = []
        for link in link_section:
            href = link['slug']
            href = "https://www.otodom.pl/pl/oferta/" + href
            hrefs.append(href)

        return hrefs


class Olx_Crawler(Crawler_Base):
    def __init__(self, city: str, type_of_offer: OFFER, type_of_estate: ESTATE,
                 session_retry_connect: int = 15, 
                 session_retry_read: int = 15, 
                 session_retry_redirect: int = 15) -> None:
        super().__init__(city, type_of_offer, type_of_estate, 
                         tag_name="h3", class_name="css-1y5481k er34gjf0",
                         session_retry_connect=session_retry_connect, 
                         session_retry_read=session_retry_read, 
                         session_retry_redirect=session_retry_redirect)
        

    def _get_url(self, page_no: int, type_of_offer: OFFER, type_of_estate: ESTATE, city: str) -> str:
        if type_of_offer == OFFER.RENT:
            str_offer = "wynajem"
        elif type_of_offer == OFFER.SALE:
            str_offer = "sprzedaz"

        if type_of_estate == ESTATE.APARTMENT:
            str_estate = "mieszkania"
        elif type_of_estate == ESTATE.HOUSE:
            str_estate = "domy"

        if city == "cala-polska":
            #TODO: shows only 25 page !!!
            pass
        
        return f"https://www.olx.pl/nieruchomosci/{str_estate}/{str_offer}/{city}/?page={int(page_no)}"


    def _get_link_section(self, soup: bs4.BeautifulSoup) -> Union[bs4.element.ResultSet, List[str]]:          
        return soup.find_all("a", class_="css-rc5s2u")


    def _get_links(self, link_section: Union[bs4.element.ResultSet, List[str]]) -> List[str]:
        hrefs = []
        for link in link_section:
            href = link['href']
            if href.startswith("/d/"):
                href = "https://www.olx.pl" + href
            else:
                href = href.replace(".html", "")
            hrefs.append(href)

        return hrefs