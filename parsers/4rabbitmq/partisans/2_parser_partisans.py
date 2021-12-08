import os
import re
from bs4 import BeautifulSoup
import requests
import time
import pika
import secrets
import pymongo


INPUT_QUEUE = 'parse_partisans_url'

def get_html_from_base(url): 
    page_data = db_client['biographydb']['downloaded_pages'].find_one({'url': url})
    return page_data['body'], page_data['updated_at']

def get_partisan_info(html_text, url, updated_at):
    soup = BeautifulSoup(html_text)
    credentials = {}
    name = soup.find("h1", attrs={'class': "info-book__title"}).text.strip()
    name = name.split(sep = " ", maxsplit=3)
    bio = soup.find("ul", attrs={'class': "info-book__descr"}).text
    credentials["original_link"] = url
    credentials["retrieved_at"] = updated_at        
    try:
      credentials["surname"] = name[0]
    except IndexError:
      credentials["surname"] = ""
    try:
      credentials["firstname"] = name[1]
    except IndexError:
      credentials["firstname"] = ""
    try:
      credentials["patronym"] = name[2]
    except IndexError:
      credentials["patronym"] = ""
    try:
      credentials["birthdate"] = re.search(r'\d{4}', bio).group().strip()
    except AttributeError:
      credentials["birthdate"] = ""
    try:
      credentials["birthplace"] = re.search(r'(?<=Место\sрождения: \s).*', bio).group().strip()
    except AttributeError:
      credentials["birthplace"] = ""
    try:
      credentials["nationality"] = re.search(r'(?<=Национальность:\s).*', bio).group().strip()
    except AttributeError:
      credentials["nationality"] = ""
    try:
      credentials["award"] = []
      award = soup.find("div", attrs={'class': "info-book__rewards-list"})
      for rewards_item in award.find_all("div", attrs={'class': "info-book__rewards-item"}):
        credentials["award"].append(rewards_item.find("img")['alt'])
    except AttributeError:
      credentials["award"] = ""
    try:
      credentials["award_nomination"] = []
      presented = soup.find("div", attrs={'class': "info-book__rewards-inline"})
      for item in presented.find_all("span", attrs={'class': "info-book__rewards-inline-text"}):
        credentials["award_nomination"].append(item.text)
    except AttributeError:
      credentials["award_nomination"] = "" 
    try:
      credentials["time_period"] = []
      credentials["partisan_brigade"] = []
      credentials["partisan_detachment"] = []
      credentials["position"] = []
      formation__list = soup.find("div", attrs={'class': "formation__list"})
      for formation_item in formation__list.find_all("div", attrs={'class': "formation__item"}):
        credentials["time_period"].append(formation_item.find("div", attrs={'class': "formation__item-date"}).text)
        for item_descr in formation_item.find_all("ul", attrs={'class': "formation__item-descr"}):
          help = item_descr.find_all("li")
          credentials["partisan_brigade"].append(help[0].find('a').text)
          credentials["partisan_detachment"].append(help[1].find('a').text)
          credentials["position"].append(re.sub("^\nДолжность: ", "", help[2].text).strip())
    except:  
      credentials["time_period"] = ""
      credentials["partisan_brigade"] = ""
      credentials["partisan_detachment"] = ""
      credentials["position"] = ""
    return credentials
        

def callback(ch, method, properties, body):
    #print(" [x] Received %r" % body)
    #print(type(body))
    url = body.decode('utf-8')
    html_text, updated_at = get_html_from_base(url)
    time.sleep(0.1)
    print(get_partisan_info(html_text, url, updated_at)) # это надо будет передавать валидатору


def main():
    credentials = pika.PlainCredentials(secrets.RABBITMQ_USER,
                                        secrets.RABBITMQ_PASSWD)
    connection_params = pika.ConnectionParameters(secrets.RABBITMQ_HOST,
                                                  secrets.RABBITMQ_PORT,
                                                  secrets.RABBITMQ_VHOST,
                                                  credentials = credentials)
    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()
    channel.queue_declare(queue=INPUT_QUEUE)
    channel.basic_consume(queue=INPUT_QUEUE, on_message_callback=callback, auto_ack=False)
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

    
if __name__ == '__main__':
    db_client = pymongo.MongoClient(secrets.MONGO_HOST,
                            secrets.MONGO_PORT,
                            username=secrets.MONGO_WRITER_USER,
                            password=secrets.MONGO_WRITER_PASSWD)

    main()

