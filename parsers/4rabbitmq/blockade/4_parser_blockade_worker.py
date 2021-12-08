import os
import re
from bs4 import BeautifulSoup as beat_soup
import pandas as pd
import requests
import time
import pika
import secrets
import pymongo

INPUT_QUEUE = 'parse_blockade_person'

def get_html_from_base(url): 
    page_data = db_client['biographydb']['downloaded_pages'].find_one({'url': url})
    return page_data['body']

def get_text(contents: str) -> str:
    """ достаем текстовую статью с хтмл-странички """
    soup = beat_soup(contents, 'lxml')
    # странички сделаны странно и теги у них не всегда одинаковы - пробуем оба варианта
    if soup.find("div", class_ = "col-sm-12"):
        return soup.find("div", class_ = "col-sm-12").text.strip().replace("\n", "")
    return soup.find("div", class_ = "col-sm-9").text.strip().replace("\n", "")

def get_person_info(text: str) -> dict:
    DEATH_DATE = r"смерти: [1,2]?[0-9]? ?\w{3,10} 19[0-9]{2}"
    """ создаем словарь с информацией о человеке """
    person = text 
    info = {"fullname": "", "birthdate": "",
            "firstname": "", "patronym": "", "surname": "",
            "place_of_residence": "", "death_date": "", "burial_place": "",
            "source": ""}
    
    fio = person.split(",")[0] # чаще всего отделено запятой, но иногда нет... :
    if "Место" in fio:
        fio = fio.split("Место")[0].strip()
    if "Дата" in fio:
        fio = fio.split("Дата")[0].strip() 

    splitted_fio = fio.strip().split()
   # print (splitted_fio)
    if len(splitted_fio) == 3:  
        first_name, patronym, last_name = splitted_fio[1], splitted_fio[2], splitted_fio[0]
    elif len(splitted_fio) == 4:
        if splitted_fio[1][0] == "(":
            first_name, patronym, last_name = splitted_fio[2], splitted_fio[3], splitted_fio[0]+" "+splitted_fio[1]
        elif splitted_fio[2][0] == "(":
            last_name, patronym, first_name = splitted_fio[0], splitted_fio[3], splitted_fio[1]+" "+splitted_fio[2]
        elif splitted_fio[3][0] == "(":
            last_name, first_name, patronym = splitted_fio[0], splitted_fio[1], splitted_fio[2]+" "+splitted_fio[3]
        else:
            first_name, patronym, last_name = splitted_fio[1], splitted_fio[2], splitted_fio[0]
    elif len(splitted_fio) == 2:       
        first_name, last_name = splitted_fio[1], splitted_fio[0]
        patronym = "no_info"
    else:
        last_name = fio
        first_name, patronym = "", ""
    
    try:
        birth_date = re.findall(r"[0-3]?[0-9]?.?[0-1]?[0-9]?.?1[8-9][0-9]{2}", 
                                person.split("г.р.")[0].split("род")[1])[0].strip("., ")  
        # (да. выше было что-то уродливое. но оно... типа... работает)
    except:
        birth_date = "no_info" # иногда даты рождения не бывает
        
    info["fullname"], info["birthdate"] = fio, birth_date
    info["firstname"], info["patronym"], info["surname"] = first_name, patronym, last_name
    
    # дергаем место проживания, если есть
    if "Место проживания: " in person:  
        addr = person.split("Место проживания: ")[1] 
        if "Дата смерти" in person:
            address = addr.split("Дата")[0].strip(". ")
        else:
            address = addr.split("(")[0].strip(". ")
            info["place_of_residence"] = address
     
    # дергаем всякое про смерть, если есть (чаще нет)
    if "Дата смерти" in person: 
        if re.findall(DEATH_DATE, person):
            death_date = re.findall(DEATH_DATE, person)[0].replace("смерти: ", "")
            info["death_date"] = death_date 
            
    if "Место захоронения: " in person:
        try:
            burial_place = person.split("Место захоронения: ")[-1].split("(")[0]
            info["burial_place"] = burial_place
        except:
            burial_place = "no_info"
    
    # источник достается не всегда -- иногда на страничке он просто в другом блоке
    # это надо дописать
    if "(" in person:
        source = person.split("(")[-1].strip(") ")
        info["source"] = source
    
    return info

def callback(ch, method, properties, body):
    #print(" [x] Received %r" % body)
    #print(type(body))
    url = body.decode('utf-8')
    html_text = get_html_from_base(url)
    time.sleep(0.1)
    print(get_person_info(get_text(html_text)))

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

