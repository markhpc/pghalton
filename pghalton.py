#!/bin/python
import argparse
import logging
import sys
import pprint

import settings
import common
from pg import PG
from pool import Pool
from log_support import setup_loggers

logger = logging.getLogger("pghalton")

def parse_args(args):
    parser = argparse.ArgumentParser(description='Simulate PG distribution with Halton Sequences')

    parser.add_argument(
        'config_file',
        help='YAML config file.',
        )

    return parser.parse_args(args[1:])

def main(argv):
    setup_loggers()
    ctx = parse_args(argv)
    settings.initialize(ctx)

    logger.debug("Settings.general:\n    %s",
                 pprint.pformat(settings.general).replace("\n", "\n    "))

    pool = Pool()
    pool.print_counts() 

    for i in xrange(9, 10):
        print ""
        print "setting osds to: %s" % i
        pool.set_osds(i)
        pool.print_counts()

if __name__ == '__main__':
    exit(main(sys.argv))
