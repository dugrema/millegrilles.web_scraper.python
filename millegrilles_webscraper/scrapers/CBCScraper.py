import asyncio
import datetime
import feedparser
import logging

from typing import Optional

from millegrilles_webscraper.Context import WebScraperContext
from millegrilles_webscraper.scrapers.WebScraper import WebScraper


class CBCScraper(WebScraper):

    def __init__(self, context: WebScraperContext, url: str, semaphore: asyncio.BoundedSemaphore, refresh_rate: Optional[datetime.timedelta] = None):
        super().__init__(context, url, semaphore, refresh_rate)
        self.__logger = logging.getLogger(f'{__name__}.{self.__class__.__name__}')

    async def scrape(self):
        self.__logger.debug(f"Scraping {self.url}")
        result = await asyncio.to_thread(feedparser.parse, self.url, etag=self.etag, modified=self.last_update)
        print("Result\n%s" % result)

        pass

