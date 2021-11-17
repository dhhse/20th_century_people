import os
import sys
import datetime
import requests
import pymongo
import pika
import secrets

def download_page_callback(ch, method, properties, msg):
    url = msg.decode('utf-8')
    print(f"Received url {url}")
    response = requests.get(url)
    if response.status_code == 200:
        body = response.text
        now = datetime.datetime.now()
        record = db.downloaded_pages.replace_one({'url': url}, {'url': url, 'body': body, 'updated_at': now}, upsert=True)
        print(f"Stored url {url} into DB")

        ch.basic_publish(exchange='',
                          routing_key='to_parse',
                          body=url)
    else:
        print(f"Url {url} responded with HTTP-code {response.status_code}")
    ch.basic_ack(delivery_tag = method.delivery_tag)


def main():
    credentials = pika.PlainCredentials(secrets.RABBITMQ_USER,
                                        secrets.RABBITMQ_PASSWD)
    connection_params = pika.ConnectionParameters(secrets.RABBITMQ_HOST,
                                                  secrets.RABBITMQ_PORT,
                                                  secrets.RABBITMQ_VHOST,
                                                  credentials = credentials)
    queue_connection = pika.BlockingConnection(connection_params)
    channel = queue_connection.channel()
    #channel.queue_declare(queue='to_download')
    channel.basic_consume(queue='to_download', auto_ack=False, on_message_callback=download_page_callback)
    channel.start_consuming()
    print('waiting')
    channel.queue_declare(queue='to_parse')

if __name__ == '__main__':
    db_client = pymongo.MongoClient(secrets.MONGO_HOST,
                                    secrets.MONGO_PORT,
                                    username=secrets.MONGO_WRITER_USER,
                                    password=secrets.MONGO_WRITER_PASSWD)
    db = db_client['biographydb']
    if 'url_idx' not in db.downloaded_pages.index_information():
        db.downloaded_pages.create_index([('url', pymongo.ASCENDING)], name='url_idx', unique=True)
    
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
