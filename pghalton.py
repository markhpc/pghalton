#!/bin/python
import argparse
import logging
import sys
import pprint

import settings
from pg import PG
from log_support import setup_loggers

logger = logging.getLogger("pghalton")

def make_up_map(down_map):
    gs = settings.general
    up_map = []
    for osd in xrange(0, gs.get('potential_osds', 0)):
        if osd not in down_map:
            up_map.append(osd)
    return up_map
 
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

    gs = settings.general
    down_osds = []
    for d in xrange(gs.get('acting_osds', 0), gs.get('potential_osds', 0)):
       down_osds.append(d)

    up_map = make_up_map(down_osds)
    print up_map
    up_index = 0
    pg_list = []

    PG.reset_remap_counter() 
    for i in xrange(0, gs.get('pgs', 0)):
        pg = PG(i)
        pg_list.append(pg)
#        pg.print_osds()
        up_index = pg.remap(up_map, up_index)
#        pg.print_osds()
    PG.print_remap_counter()

    acting_counts = [0]*(gs.get('potential_osds', 0))
    up_counts = [0]*(gs.get('potential_osds', 0))

    for pg in pg_list:
        for osd in pg.get_acting():
            acting_counts[osd]+=1
        for osd in pg.get_up():
            up_counts[osd]+=1

    print acting_counts
    print up_counts

if __name__ == '__main__':
    exit(main(sys.argv))
