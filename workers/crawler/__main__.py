import datetime

import requests
import pymongo
import pika
import retry

from ..utils import suppress_exceptions
from ..connections import get_rabbit_connection, get_mongo_db

class Crawler:
    def __init__(self):
        self.db = get_mongo_db()
        if 'url_idx' not in self.db.downloaded_pages.index_information():
            self.db.downloaded_pages.create_index([('url', pymongo.ASCENDING)], name='url_idx', unique=True)

    @retry.retry(pymongo.errors.ConnectionFailure, delay=5, jitter=(1, 3), tries=-1)
    def store_page(self, url, body, updated_at):
        record = self.db.downloaded_pages.replace_one({'url': url}, {'url': url, 'body': body, 'updated_at': updated_at}, upsert=True)
        return record

    @suppress_exceptions([Exception], log_fail=True)
    def download_page_callback(self, ch, method, properties, msg):
        try:
            msg_text = msg.decode('utf-8')
            msg_parts = msg_text.split('\t')
            if len(msg_parts) == 1:
                url = msg_parts[0]
            elif len(msg_parts) == 2:
                url, output_queue = msg_parts
            else:
                raise Exception(f'Unknown message format: `{msg_text}`.')
            print(f"Received url {url}")
            response = requests.get(url) # ToDo: add timeout
            if response.status_code == 200:
                body = response.text
                now = datetime.datetime.now()
                record = self.store_page(url, body, now)
                print(f"Stored url {url} into DB")
                if output_queue:
                    ch.queue_declare(queue=output_queue)
                    ch.basic_publish(exchange='', routing_key=output_queue, body=url)
                    print(f"Sent url {url} into queue {output_queue}")
            else:
                print(f"Url {url} responded with HTTP-code {response.status_code}")
        except:
            ch.queue_declare(queue='failed_download')
            ch.basic_publish(exchange='', routing_key='failed_download', body=msg)
            print(f"Sent {msg} into queue `failed_download`")
        finally:
            ch.basic_ack(delivery_tag = method.delivery_tag)


    @retry.retry(pika.exceptions.AMQPConnectionError, delay=5, jitter=(1, 3), tries=-1)
    def consume(self, callback):
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

    def start(self):
        self.consume(self.download_page_callback)

if __name__ == '__main__':
    crawler = Crawler()
    crawler.start()
