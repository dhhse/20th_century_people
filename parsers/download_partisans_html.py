import requests
import time
from bs4 import BeautifulSoup

def download_partisans_html(number):
    page = "http://partizany.by/partisans/" + str(number)
    while True:
        try:
            page = requests.get(page, verify=False)
            break
        except:
            time.sleep(60)
    soup = BeautifulSoup(page.text)
    if soup.find("div", {"class": "page-404__text"}) is None:
        with open("links/" + str(number) + ".html", "w", encoding='utf-8') as f:
            f.write(page.text)
    return number