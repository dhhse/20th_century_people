import logging
import pika

logger = logging.getLogger('workers_logger')

class AMQPWorkerHarness:
    def __init__(self, secrets):
        self.connection = self.make_connection(secrets)
        self.channel = self.connection.channel()

    def make_connection(self, secrets):
        credentials = pika.PlainCredentials(secrets['RABBITMQ_USER'],
                                            secrets['RABBITMQ_PASSWD'])
        connection_params = pika.ConnectionParameters(secrets['RABBITMQ_HOST'],
                                                      secrets['RABBITMQ_PORT'],
                                                      secrets['RABBITMQ_VHOST'],
                                                      credentials=credentials)
        return pika.BlockingConnection(connection_params)

    def seed(self):
        pass

    def callback_list(self):
        return {}

    def start(self):
        for queue_to_listen, stage_callback in self.callback_list().items():
            self.channel.queue_declare(queue=queue_to_listen)
            self.channel.basic_consume(queue=queue_to_listen,
                on_message_callback=stage_callback,
                auto_ack=False)
            logger.info(f' [*] {queue_to_listen} is waiting for messages')
        self.seed()
        self.channel.start_consuming()

    def send_message(self, queue, msg):
        self.channel.queue_declare(queue=queue)
        self.channel.basic_publish(exchange='', routing_key=queue, body=msg)

# simple_callback(self, msg) -> complex_callback(self, channel, method, properties, msg)
def amqp_callback(func):
    def wrapper(self, channel, method, properties, msg):
        logger.debug(f'called {func} with message `{msg}`')
        msg = msg.decode('utf-8')
        func(self, msg)
        channel.basic_ack(delivery_tag = method.delivery_tag)
    return wrapper
