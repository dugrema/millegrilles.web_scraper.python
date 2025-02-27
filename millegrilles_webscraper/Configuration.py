import argparse
import os
import logging

from millegrilles_messages.bus.BusConfiguration import MilleGrillesBusConfiguration

# Configuration loader.
# Use: configuration = FileHostConfiguration.load()

# Environment variables
ENV_DIR_DATA = 'DIR_DATA'
ENV_FILEHOST_WEB_URL = 'FILEHOST_WEB_URL'

# Default values
DEFAULT_DIR_DATA="/var/opt/millegrilles/web_scraper/data"
DEFAULT_FILEHOST_WEB_URL = 'https://filehost:1443/'


def _parse_command_line():
    parser = argparse.ArgumentParser(description="Web scraper for MilleGrilles")
    parser.add_argument(
        '--verbose', action="store_true", required=False,
        help="More logging"
    )

    args = parser.parse_args()
    __adjust_logging(args)
    return args


LOGGING_NAMES = [__name__, 'millegrilles_messages', 'millegrilles_webscraper']


def __adjust_logging(args: argparse.Namespace):
    logging.basicConfig()
    if args.verbose is True:
        for log in LOGGING_NAMES:
            logging.getLogger(log).setLevel(logging.DEBUG)
    else:
        for log in LOGGING_NAMES:
            logging.getLogger(log).setLevel(logging.INFO)


class WebScraperConfiguration(MilleGrillesBusConfiguration):

    def __init__(self):
        super().__init__()
        self.dir_data = DEFAULT_DIR_DATA
        self.filehost_web_url = DEFAULT_FILEHOST_WEB_URL

    def parse_config(self):
        super().parse_config()
        self.dir_data = os.environ.get(ENV_DIR_DATA) or self.dir_data
        self.filehost_web_url = os.environ.get(ENV_FILEHOST_WEB_URL) or self.filehost_web_url

    @staticmethod
    def load():
        config = WebScraperConfiguration()
        _parse_command_line()
        config.parse_config()
        return config
