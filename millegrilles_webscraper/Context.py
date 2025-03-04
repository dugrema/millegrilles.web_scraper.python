import logging

from typing import Optional

from millegrilles_webscraper.Configuration import WebScraperConfiguration
from millegrilles_messages.bus.BusContext import MilleGrillesBusContext
from millegrilles_messages.bus.PikaConnector import MilleGrillesPikaConnector
from millegrilles_webscraper.DataStructures import AttachedFileInterface

LOGGER = logging.getLogger(__name__)


class WebScraperContext(MilleGrillesBusContext):

    def __init__(self, configuration: WebScraperConfiguration):
        super().__init__(configuration)
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__bus_connector: Optional[MilleGrillesPikaConnector] = None
        self.__file_handler: Optional[AttachedFileInterface] = None
        self.__scrape_throttle_seconds: Optional[int] = 5

    @property
    def bus_connector(self):
        return self.__bus_connector

    @bus_connector.setter
    def bus_connector(self, value: MilleGrillesPikaConnector):
        self.__bus_connector = value

    @property
    def file_handler(self):
        return self.__file_handler

    @file_handler.setter
    def file_handler(self, value: AttachedFileInterface):
        self.__file_handler = value

    async def get_producer(self):
        return await self.__bus_connector.get_producer()

    @property
    def scrape_throttle_seconds(self) -> Optional[int]:
        return self.__scrape_throttle_seconds
