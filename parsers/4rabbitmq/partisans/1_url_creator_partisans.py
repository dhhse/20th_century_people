import pika
import time
import random
import secrets

OUTPUT_QUEUE = 'to_download'
FORWARD_QUEUE_FOR_CRAWLER = 'parse_partisans_url'

def main():
    credentials = pika.PlainCredentials(secrets.RABBITMQ_USER,
                                        secrets.RABBITMQ_PASSWD)
    connection_params = pika.ConnectionParameters(secrets.RABBITMQ_HOST,
                                                  secrets.RABBITMQ_PORT,
                                                  secrets.RABBITMQ_VHOST,
                                                  credentials = credentials)
    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()
    channel.queue_declare(queue=OUTPUT_QUEUE)


    for num in range(23601, 170000):
        time.sleep(0.1 * random.randint(1,5))
        url = f'https://partizany.by/partisans/{num}/'
        channel.basic_publish(exchange='',
              routing_key=OUTPUT_QUEUE,
              body='\t'.join((url, FORWARD_QUEUE_FOR_CRAWLER)))
        print(" [x] Sent data")
    connection.close()


if __name__ == '__main__':
    main()
