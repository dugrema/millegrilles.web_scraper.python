import asyncio
import datetime
from io import BytesIO

import aiohttp
import feedparser
import logging

from typing import Optional

from feedparser import FeedParserDict
from xml.etree import ElementTree as ET

from millegrilles_webscraper.Context import WebScraperContext
from millegrilles_webscraper.scrapers.WebScraper import WebScraper


class ScrapedItem:

    def __init__(self, title: str, url: str, date: Optional[str]):
        self.title = title
        self.url = url
        self.date = date
        self.picture: Optional[str] = None
        self.picture_source: Optional[str] = None


class GoogleTrendsScraper(WebScraper):

    def __init__(self, context: WebScraperContext, url: str, semaphore: asyncio.BoundedSemaphore, refresh_rate: Optional[datetime.timedelta] = None):
        super().__init__(context, url, semaphore, refresh_rate)
        self.__logger = logging.getLogger(f'{__name__}.{self.__class__.__name__}')

    async def scrape(self):
        self.__logger.debug(f"Scraping {self.url}")

        content = await self.get_content()
        print("Result\n%s" % content)

        pass

    async def get_content(self) -> list[ScrapedItem]:
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url) as response:
                response.raise_for_status()
                content = await response.read()

        parsed_content = await self.extract_content(content)

        return parsed_content

    async def extract_content(self, content: bytes) -> list[ScrapedItem]:
        source = BytesIO(content)
        parsed_content: ET = ET.parse(source)

        ns_ht = 'https://trends.google.com/trending/rss'

        root = parsed_content.getroot()

        title: Optional[str] = None
        pub_date_str: Optional[str] = None
        approx_traffic: Optional[str] = None
        item_picture: Optional[str] = None
        item_picture_source: Optional[str] = None

        scraped_items_list: list[ScrapedItem] = list()

        for item in root.findall('./channel/item'):
            for child in item:
                if child.tag == '{%s}news_item' % ns_ht:
                    news_item_title: Optional[str] = None
                    news_item_url: Optional[str] = None
                    news_item_picture: Optional[str] = None
                    news_item_picture_source: Optional[str] = None

                    for news_item in child:
                        if news_item.tag == '{%s}news_item_title' % ns_ht:
                            news_item_title = news_item.text
                        elif news_item.tag == '{%s}news_item_url' % ns_ht:
                            news_item_url = news_item.text
                        elif child.tag == '{%s}news_item_picture' % ns_ht:
                            news_item_picture = child.text
                        elif child.tag == '{%s}news_item_source' % ns_ht:
                            news_item_picture_source = child.text

                    news_item_scraped = ScrapedItem(news_item_title, news_item_url, pub_date_str)
                    news_item_scraped.picture = news_item_picture or item_picture
                    news_item_scraped.picture_source = news_item_picture_source or item_picture_source

                    scraped_items_list.append(news_item_scraped)

                elif child.tag == 'title':
                    title = child.text
                elif child.tag == 'pubDate':
                    pub_date_str = child.text
                elif child.tag == '{%s}approx_traffic' % ns_ht:
                    approx_traffic = child.text
                elif child.tag == '{%s}picture' % ns_ht:
                    item_picture = child.text
                elif child.tag == '{%s}picture_source' % ns_ht:
                    item_picture_source = child.text
                pass


        return scraped_items_list
