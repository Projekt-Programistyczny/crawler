from src.crawler import Olx_Crawler, OtoDom_Crawler, OFFER, ESTATE
import json
import os
import time

SAVE_PATH = "results"


def TEST_API(city):
    json_dict = []

    
    crawler1 = Olx_Crawler(type_of_offer = OFFER.RENT, type_of_estate=ESTATE.APARTMENT, city=city)
    crawler2 = OtoDom_Crawler(type_of_offer = OFFER.RENT, type_of_estate=ESTATE.APARTMENT, city=city)

    print("Start search for Olx links...")
    x = crawler1.run()
    print()
    print("Start search for OtoDom links...")
    y = crawler2.run()
    all = x + y

    unique_list = list(set(all))

    for i in unique_list:
        json_item = {"Link": i,
                     "City": city,
                     "Type_of_estate": ESTATE.APARTMENT.value,
                     "Type_of_offer": OFFER.RENT.value}
        json_dict.append(json_item)

    if not os.path.exists(SAVE_PATH):
        os.makedirs(SAVE_PATH)

    with open(f'{SAVE_PATH}/crawler_result_{city}.json', 'w') as f:
        json.dump(json_dict, f, indent=4)


while True:
    TEST_API('katowice')
    time.sleep(3600)
