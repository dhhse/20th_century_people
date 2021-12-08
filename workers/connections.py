import pika
from ..secrets import *

def get_rabbit_connection():
    credentials = pika.PlainCredentials(secrets.RABBITMQ_USER,
                                        secrets.RABBITMQ_PASSWD)
    connection_params = pika.ConnectionParameters(secrets.RABBITMQ_HOST,
                                                  secrets.RABBITMQ_PORT,
                                                  secrets.RABBITMQ_VHOST,
                                                  credentials = credentials)
    return pika.BlockingConnection(connection_params)

def get_mongo_db():
    db_client = pymongo.MongoClient(secrets.MONGO_HOST,
                                    secrets.MONGO_PORT,
                                    username=secrets.MONGO_WRITER_USER,
                                    password=secrets.MONGO_WRITER_PASSWD,
                                    authSource=secrets.MONGO_DATABASE)
    return db_client[secrets.MONGO_DATABASE]
