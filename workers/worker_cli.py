import sys
import argparse
import json
import logging

class WorkerCLI:
    def __init__(self, logger=None):
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger('')

    def parse_cmdline(self, argv):
        parser = self.argparser()
        args = parser.parse_args(argv)
        if not args.secrets_fn:
            self.logger.error('Specify JSON-file with secrets.')
            raise Exception("Secrets should be specified using `--secrets FILE`")
        return args

    def argparser(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--secrets', dest='secrets_fn', metavar='FILE', type=str, default=None)
        self.configure_argparser(parser)
        return parser

    def configure_argparser(self, parser):
        pass

    def read_secrets(self, filename):
        with open(filename) as f:
            return json.load(f)

    def configure(self, argv=None):
        if argv is None:
            argv = sys.argv[1:]
        self.options = self.parse_cmdline(argv)
        self.secrets = self.read_secrets(self.options.secrets_fn)
