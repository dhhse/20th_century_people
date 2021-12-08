import os
import re
from bs4 import BeautifulSoup
import pandas as pd
import requests
import time
import pika
import secrets
import pymongo

INPUT_QUEUE = 'parse_blockade_seed_url'
OUTPUT_QUEUE = 'parse_blockade_person'

def get_html_from_base(url): 
    page_data = db_client['biographydb']['downloaded_pages'].find_one({'url': url})
    return page_data['body']

def callback(ch, method, properties, body):
    url = body.decode('utf-8')
    #print(url)
    html_text = get_html_from_base(url)
    soup = BeautifulSoup(html_text)
    for tag in soup.find_all('a', class_='more'):
        url = tag['href']
        ch.basic_publish(exchange='',
              routing_key='to_download',
              body='\t'.join((url, OUTPUT_QUEUE)))
            
    time.sleep(0.1)

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
