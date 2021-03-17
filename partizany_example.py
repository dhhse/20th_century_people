import os
import re
import urllib.request
import json
from bs4 import BeautifulSoup

first_url = 'https://partizany.by/partisans/154065/'

first_text = urllib.request.urlopen(first_url)
soup = BeautifulSoup(first_text.read().decode('utf-8', 'ignore'))
credentials = {}
name = soup.find("h1", attrs={'class': "info-book__title"}).text.strip()
credentials["name"] = name

bio = soup.find("ul", attrs={'class': "info-book__descr"}).text
birth = re.search(r'\d{4}', bio).group()
credentials["birth_year"] = birth
place_of_birth = re.search(r'(?<=Место\sрождения: \s).*', bio).group()
credentials["place_of_birth"] = place_of_birth
nationality = re.search(r'(?<=Национальность:\s).*', bio).group()
credentials["nationality"] = nationality

with open('credentials.json', 'w', encoding='utf-8') as f:
    json.dump(credentials, f, ensure_ascii=False, indent=4)
