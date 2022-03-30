import sys
import logging

from .amqp_worker_harness import AMQPWorkerHarness, amqp_callback
from .worker_cli import WorkerCLI


# The main class for workers orchestration
class SampleWorker(AMQPWorkerHarness):
    def __init__(self, secrets, options, logger=None):
        super().__init__(secrets)
        self.options = options
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger('')

    def callback_list(self):
        return {
            'SampleWorker_stage1': self.stage_1, # if necessary
            'SampleWorker_stage2': self.stage_2, # if necessary
        }

    def seed(self):
        msg_to_send = self.options.seed_msg
        self.logger.info(f'Seed sends message "{msg_to_send}" -> ...')
        self.send_message('SampleWorker_stage1', msg_to_send)

    @amqp_callback
    def stage_1(self, msg):
        self.logger.info(f'.. -> Stage 1 got message: "{msg}"')
        msg_to_send = "go ahead"
        self.logger.info(f'Sending message "{msg_to_send}" -> ...')
        self.send_message('SampleWorker_stage2', msg_to_send)

    @amqp_callback
    def stage_2(self, msg):
        self.logger.info(f'... -> Stage 2 got message: "{msg}". Finish')


# This class is designed to parse command line arguments
class SampleWorkerCLI(WorkerCLI):
    def configure_argparser(self, argparser):
        argparser.description = 'Sample worker'
        argparser.prog = 'python3 -m workers.sample_worker --secrets secrets.json [options]'
        argparser.add_argument('--seed-msg', dest='seed_msg', metavar='MESSAGE', type=str, default="Let's go")


if __name__ == '__main__':
    FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=FORMAT, level=logging.INFO)
    logger = logging.getLogger('workers_logger')
    cli = SampleWorkerCLI(logger=logger)
    cli.configure(sys.argv[1:])
    worker = SampleWorker(cli.secrets, cli.options, logger=logger)
    try:
        worker.start()
    except KeyboardInterrupt:
        logger.info('Worker harness stopped')
