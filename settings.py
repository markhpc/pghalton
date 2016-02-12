import argparse
import yaml
import sys
import os
import logging

logger = logging.getLogger("pghalton")

general = {}

def initialize(ctx):
    global general

    config = {}
    try:
        with file(ctx.config_file) as f:
            map(config.update, yaml.safe_load_all(f))
    except IOError, e:
        raise argparse.ArgumentTypeError(str(e))

    general = config.get('general', {})
    if not general:
        shutdown('No general section found in config file, bailing.')
