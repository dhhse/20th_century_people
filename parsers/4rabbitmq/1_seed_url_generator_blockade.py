import pika
import time
import random
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
    output_queue_for_crawler = 'parse_blockade_seed_url'

    for num in range(61):
        time.sleep(0.1 * random.randint(1,5))
        url = f'http://visz.nlr.ru/blockade/withinfo/0/{num}00'
        channel.basic_publish(exchange='',
              routing_key='to_download',
              body='\t'.join((url, output_queue_for_crawler)))
        print(" [x] Sent data")
    connection.close()


if __name__ == '__main__':
    main()
