import pika
import re
import time
import requests
from bs4 import BeautifulSoup
import secrets

def main():
    credentials = pika.PlainCredentials(secrets.RABBITMQ_USER,
                                        secrets.RABBITMQ_PASSWD)
    connection_params = pika.ConnectionParameters(secrets.RABBITMQ_HOST,
                                                  secrets.RABBITMQ_PORT,
                                                  secrets.RABBITMQ_VHOST,
                                                  credentials = credentials)
    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()
    channel.queue_declare(queue='to_download')

    for num in range(2): #must be 61 return!!
        time.sleep(1)
        url = f'http://visz.nlr.ru/blockade/withinfo/0/{num}00'
        res = requests.get(url)
        soup = BeautifulSoup(res.text, 'lxml')
        for tag in soup.find_all('a', class_='more'):
            url = tag['href']
            channel.basic_publish(exchange='',
                          routing_key='to_download',
                          body=url)
            print(" [x] Sent data")
    #print(" [x] Sent data")
    connection.close()


if __name__ == '__main__':
    main()
