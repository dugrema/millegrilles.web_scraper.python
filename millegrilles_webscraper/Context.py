import logging

from typing import Optional

from millegrilles_webscraper.Configuration import WebScraperConfiguration
from millegrilles_messages.bus.BusContext import MilleGrillesBusContext
from millegrilles_messages.bus.PikaConnector import MilleGrillesPikaConnector

LOGGER = logging.getLogger(__name__)


class WebScraperContext(MilleGrillesBusContext):

    def __init__(self, configuration: WebScraperConfiguration):
        super().__init__(configuration)
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__bus_connector: Optional[MilleGrillesPikaConnector] = None

    @property
    def bus_connector(self):
        return self.__bus_connector

    @bus_connector.setter
    def bus_connector(self, value: MilleGrillesPikaConnector):
        self.__bus_connector = value

    async def get_producer(self):
        return await self.__bus_connector.get_producer()
