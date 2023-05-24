import unittest
from unittest.mock import Mock, patch
from bs4 import BeautifulSoup
from src.crawler import Crawler_Base, OtoDom_Crawler, Olx_Crawler, OFFER, ESTATE


class TestCrawler(unittest.TestCase):
    def setUp(self):
        self.type_of_offer = OFFER.RENT
        self.type_of_estate = ESTATE.APARTMENT
        self.city = "katowice"
        self.otodom_crawler = OtoDom_Crawler(
            type_of_offer=self.type_of_offer, type_of_estate=self.type_of_estate, city=self.city)
        self.olx_crawler = Olx_Crawler(
            type_of_offer=self.type_of_offer, type_of_estate=self.type_of_estate, city=self.city)

    def test_otodom_get_url(self):
        result = self.otodom_crawler._get_url(
            page_no=1, type_of_offer=self.type_of_offer, type_of_estate=self.type_of_estate, city=self.city)
        expected_output = 'https://www.otodom.pl/pl/oferty/wynajem/mieszkanie/katowice?distanceRadius=0&page=1&limit=36&by=DEFAULT&direction=DESC&viewType=listing'

        self.assertEqual(result, expected_output)

    def test_olx_get_url(self):
        result = self.olx_crawler._get_url(
            page_no=1, type_of_offer=self.type_of_offer, type_of_estate=self.type_of_estate, city=self.city)
        expected_output = 'https://www.olx.pl/nieruchomosci/mieszkania/wynajem/katowice/?page=1'
        print(result)
        self.assertEqual(result, expected_output)

    def test_otodom_get_link_section(self):
        soup = self.otodom_crawler._get_page(1)
        result = self.otodom_crawler._get_link_section(soup)
        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)

    def test_olx_get_link_section(self):
        soup = self.olx_crawler._get_page(1)
        result = self.olx_crawler._get_link_section(soup)
        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)

    def test_otodom_get_links(self):
        soup = self.otodom_crawler._get_page(1)
        link_section = self.otodom_crawler._get_link_section(soup)
        result = self.otodom_crawler._get_links(link_section)
        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)

    def test_olx_get_links(self):
        soup = self.olx_crawler._get_page(1)
        link_section = self.olx_crawler._get_link_section(soup)
        result = self.olx_crawler._get_links(link_section)
        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)


if __name__ == "__main__":
    unittest.main()
