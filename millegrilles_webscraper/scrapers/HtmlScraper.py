import asyncio
import datetime
import feedparser
import logging

from typing import Optional

from millegrilles_webscraper.Context import WebScraperContext
from millegrilles_webscraper.scrapers.WebScraper import WebScraper


class HtmlScraper(WebScraper):

    def __init__(self, context: WebScraperContext, url: str, semaphore: asyncio.BoundedSemaphore, refresh_rate: Optional[datetime.timedelta] = None):
        super().__init__(context, url, semaphore, refresh_rate)
        self.__logger = logging.getLogger(f'{__name__}.{self.__class__.__name__}')

    async def scrape(self):
        self.__logger.debug(f"Scraping {self.url}")
        result = await asyncio.to_thread(feedparser.parse, self.url, etag=self.etag, modified=self.last_update)
        print("Result\n%s" % result)

        pass

    async def generate_pdf(self):
        # chromium --headless --disable-gpu --run-all-compositor-stages-before-draw
        # --virtual-time-budget=10000
        # --print-to-pdf="article_X.pdf"
        # https://www.theglobeandmail.com/business/article-td-bank-posts-lower-profit-beats-estimates-with-loan-loss-provisions/
        pass