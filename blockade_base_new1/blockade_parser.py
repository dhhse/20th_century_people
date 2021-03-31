import re
import requests
from tqdm import tqdm
from bs4 import BeautifulSoup


for num in tqdm(range(3252, 3518)):
    url = f'http://visz.nlr.ru/blockade/withinfo/0/{num}00'
    res = requests.get(url)
    soup = BeautifulSoup(res.text)
    for tag in soup.find_all('a', class_='more'):
        url = tag['href']
        res = requests.get(url)
        pers_id = re.search(r'\d+', url)[0]
        with open(f'{pers_id}.html', 'w') as fw:
            fw.write(res.text)
