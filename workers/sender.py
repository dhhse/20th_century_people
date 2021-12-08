import os
import sys
import datetime
import functools
import json
import csv
import argparse
import time
import random

import pika

from .connections import get_rabbit_connection

def sender_loop(queue_name):
    try:
        connection = get_rabbit_connection()
        channel = connection.channel()
        channel.queue_declare(queue=queue_name)
        while True:
            msg = input()
            sent = False
            while not sent:
                try:
                    channel.basic_publish(exchange='', routing_key=queue_name, body=msg)
                    sent = True
                except pika.exceptions.AMQPConnectionError:
                    time.sleep(5 + random.randint(1,3))
                    connection = get_rabbit_connection()
                    channel = connection.channel()
                    channel.queue_declare(queue=queue_name)
    except KeyboardInterrupt:
        connection.close()
        print('Interrupted')

def main():
    parser = argparse.ArgumentParser(description='Send messages to a queue', prog='python3 -m workers.sender')
    parser.add_argument('--queue', dest='queue_name', metavar='NAME', type=str, default=None)
    args = parser.parse_args()
    if not args.queue_name:
        raise Exception('Specify queue name')
    sender_loop(args.queue_name)

if __name__ == '__main__':
    main()
