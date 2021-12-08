import pika
import pymongo
from ..secrets import *

def get_rabbit_connection():
    credentials = pika.PlainCredentials(RABBITMQ_USER,
                                        RABBITMQ_PASSWD)
    connection_params = pika.ConnectionParameters(RABBITMQ_HOST,
                                                  RABBITMQ_PORT,
                                                  RABBITMQ_VHOST,
                                                  credentials = credentials)
    return pika.BlockingConnection(connection_params)

def get_mongo_db():
    db_client = pymongo.MongoClient(MONGO_HOST,
                                    MONGO_PORT,
                                    username=MONGO_WRITER_USER,
                                    password=MONGO_WRITER_PASSWD,
                                    authSource=MONGO_DATABASE)
    return db_client[MONGO_DATABASE]
