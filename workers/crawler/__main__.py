import os
import sys
import datetime
import functools

import requests
import pymongo
import pika
import retry

from ..utils import suppress_exceptions
from ..connections import get_rabbit_connection, get_mongo_db

@retry.retry(pymongo.errors.ConnectionFailure, delay=5, jitter=(1, 3), tries=-1)
def store_page(db, url, body, updated_at):
    record = db.downloaded_pages.replace_one({'url': url}, {'url': url, 'body': body, 'updated_at': updated_at}, upsert=True)
    return record

@suppress_exceptions([Exception], log_fail=True)
def download_page_callback(ch, method, properties, msg, db=None):
    url, output_queue = msg.decode('utf-8').split('\t')

    print(f"Received url {url}")
    response = requests.get(url) # ToDo: add timeout
    if response.status_code == 200:
        body = response.text
        now = datetime.datetime.now()
        record = store_page(db, url, body, now)
        print(f"Stored url {url} into DB")

        ch.queue_declare(queue=output_queue)
        ch.basic_publish(exchange='',
                          routing_key=output_queue,
                          body=url)
    else:
        print(f"Url {url} responded with HTTP-code {response.status_code}")
        # ToDo: store failed downloads somewhere
    ch.basic_ack(delivery_tag = method.delivery_tag)

@retry.retry(pika.exceptions.AMQPConnectionError, delay=5, jitter=(1, 3), tries=-1)
def consume(callback):
    connection = get_rabbit_connection()
    channel = connection.channel()
    channel.queue_declare(queue='to_download')
    channel.basic_consume(queue='to_download', auto_ack=False, on_message_callback=callback)
    try:
        print('start consuming')
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
        connection.close()
        print('Interrupted')

def main():
    db = get_mongo_db()
    if 'url_idx' not in db.downloaded_pages.index_information():
        db.downloaded_pages.create_index([('url', pymongo.ASCENDING)], name='url_idx', unique=True)
    callback = functools.partial(download_page_callback, db=db)
    consume(callback)

if __name__ == '__main__':
    main()
