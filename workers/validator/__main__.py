import os
import sys
import datetime
import functools
import json
import csv
import argparse

import pymongo
import pika
import retry

from ..utils import suppress_exceptions
from ..connections import get_rabbit_connection, get_mongo_db

from .validation import read_validation_config, RecordValidator
from .validation_logging import EntryLogJournal, MissingFieldEntry, ExcessFieldEntry

class MongoLogger:
    def __init__(self, collection):
        self.collection = collection
    def write(self, message, record=None):
        now = datetime.datetime.now()
        log_entry = {'message': message, 'record': record, 'created_at': now}
        self.collection.insert_one(log_entry)
        print(log_entry, file=sys.stderr)

@suppress_exceptions([Exception], log_fail=True)
def validate_record_callback(ch, method, properties, msg, db=None, logger=None, validator=None):
    try:
        record = json.loads(msg.decode('utf-8'))
    except:
        logger.write(f"Can't decode incorrect JSON. Discarded", msg)
        ch.basic_ack(delivery_tag = method.delivery_tag)
        raise
    validation_log = validator.validate(record)
    if validation_log.is_valid():
        db.originalRecords.replace_one({'id': record['id']}, record, upsert=True)
        print(f'updated record {record}')
    else:
        error_log = validation_log.filtered_by_level("error")
        for log_entry in error_log:
            logger.write(str(log_entry), record)
    ch.basic_ack(delivery_tag = method.delivery_tag)

@retry.retry(pika.exceptions.AMQPConnectionError, delay=5, jitter=(1, 3))
def consume(callback):
    connection = get_rabbit_connection()
    channel = connection.channel()
    channel.queue_declare(queue='to_validate')
    channel.basic_consume(queue='to_validate', auto_ack=False, on_message_callback=callback)
    try:
        print('start consuming')
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
        connection.close()
        print('Interrupted')
        return

def main():
    db = get_mongo_db()
    logger = MongoLogger(db['validation_log'])

    parser = argparse.ArgumentParser(description='Transform key names', prog='python3 -m app.converter')
    parser.add_argument('--validation', dest='validation_filename', metavar='FILE', type=str, default=None)
    args = parser.parse_args()
    if not args.validation_filename:
        raise Exception('Specify validation config file')
    validation_config = read_validation_config(args.validation_filename)
    validator = RecordValidator(validation_config)

    callback = functools.partial(validate_record_callback, db=db, logger=logger, validator=validator)
    consume(callback)

if __name__ == '__main__':
    main()
