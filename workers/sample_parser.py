import sys
import json
import argparse
import logging

from .amqp_worker_harness import AMQPWorkerHarness, amqp_callback
from db_storage import db_connection, get_html_from_base
from .parsing import Parser

DOWNLOAD_QUEUE = 'to_download'
STORE_QUEUE = 'store_to_db'

# seed -- (url + stage_1) --> *download_queue* --> stage_1 --> stage_2 --> *output_queue*
class SampleParser(AMQPWorkerHarness):
    def __init__(self, secrets, options, logger=None):
        super().__init__(secrets)
        self.options = options
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger('')

    def callback_list(self):
        return {
            'SampleParser_deferred_seed': self.deferred_seed, # to make seed asynchronous
            'SampleParser_stage1': self.stage_1, # if necessary
            'SampleParser_stage2': self.stage_2, # if necessary
            'SampleParser_parse':  self.parse_and_store,    # obligatory
        }

    def seed(self):
        # seed will send the only message, thus all the workers (both deferred_seed and all the rest)
        # will be run immediately
        self.send_message('SampleParser_deferred_seed', 'start')

    @amqp_callback
    def deferred_seed(self, msg): # we can ignore message
        # Seed pages which are to be downloaded
        next_stage_queue = 'SampleParser_stage1'
        for i in range(1000):
            url = f'https://some.site.org/url/{i}'
            self.send_message(self.options.download_queue, f'{url}\t{next_stage_queue}\tsend_content')
            # or
            # self.send_message(self.options.download_queue, f'{url}\t{next_stage_queue}\tsend_url')
            logger.info(f'Sent {url} to {self.options.download_queue}. Result will be redirected to {next_stage_queue}.')

        # Alternative: table loading
        with open('table.tsv') as f:
            reader = csv.DictReader(f)
            for idx, line in enumerate(reader, start=1):
                self.send_message('SampleParser_parse', line)
                logger.info(f'Sent row {idx} from table to SampleParser_parse.')

    @amqp_callback
    def stage_1(self, msg):
        logger.info(f'stage_1 got message `{msg}`')
        result = 'some_data'  # do smth with msg
        self.send_message('SampleParser_stage2', result)
        logger.info(f'stage_1 sent message `{result}` to SampleParser_stage2')

    @amqp_callback
    def stage_2(self, msg):
        logger.info(f'stage_2 got message `{msg}`')
        result = 'document_text' # do smth with msg
        self.send_message('SampleParser_parse', result)
        logger.info(f'stage_2 sent message `{result}` to SampleParser_parse')

    @amqp_callback
    def parse_and_store(self, msg):
        logger.info(f'parse_and_store got message `{msg}`')
        data = self.parse(msg)
        self.send_message(self.options.output_queue, data)
        logger.info(f'parse_and_store sent message `{data}` to {self.options.output_queue}')

    def parse(self, msg):
        # do smth with msg (content or url) and obtain json
        return 'document_json'

class SampleParserCLI(WorkerCLI):
    def configure_argparser(self, argparser):
        argparser.description = 'Sample parser'
        argparser.prog = 'python3 -m workers.sample_parser --secrets secrets.json [options]'
        argparser.add_argument('--download-queue', dest='download_queue', metavar='QUEUE', type=str, default='to_download')
        argparser.add_argument('--output-queue', dest='output_queue', metavar='QUEUE', type=str, default='store_to_db')


if __name__ == '__main__':
    FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=FORMAT, level=logging.INFO)
    logger = logging.getLogger('workers_logger')

    cli = SampleParserCLI(logger=logger)
    cli.configure(sys.argv[1:])
    worker = SampleParser(cli.secrets, cli.options, logger=logger)
    try:
        worker.start()
    except KeyboardInterrupt:
        logger.info('Parser stopped')
