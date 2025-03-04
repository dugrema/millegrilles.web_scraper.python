import asyncio
import datetime

from typing import Optional, TypedDict

import pytz

from millegrilles_messages.chiffrage.EncryptionKey import generate_new_secret
from millegrilles_webscraper.Context import WebScraperContext


class FeedInformation(TypedDict):
    name: str
    url: Optional[str]
    auth_username: Optional[str]
    auth_password: Optional[str]


class FeedParametersType(TypedDict):
    feed_id: str
    feed_type: str
    security_level: str
    domain: str
    poll_rate: Optional[int]
    active: bool
    decrypt_in_database: bool
    encrypted_feed_information: dict
    decrypted_feed_information: Optional[dict]
    deleted: bool


class WebScraper:

    def __init__(self, context: WebScraperContext, feed: FeedParametersType, semaphore: asyncio.BoundedSemaphore):
        self._context = context
        self.__semaphore = semaphore
        self.__feed = feed

        self.__url = feed['decrypted_feed_information']['url']
        poll_rate = feed.get('poll_rate')
        if poll_rate:
            if poll_rate < 30:
                poll_rate = 30  # Minimum polling of 30 seconds
            self.__refresh_rate: Optional[datetime.timedelta] = datetime.timedelta(seconds=poll_rate)
        else:
            self.__refresh_rate = None

        self.__stop_event = asyncio.Event()

        self.__last_update: Optional[datetime.datetime] = None
        self.__etag: Optional[str] = None

        ca_certificate = context.ca
        domains = ['DataCollector']
        domain = self.__feed.get('domain')
        if domain and domain != 'DataCollector':
            domains.append(domain)

        self._encryption_key = generate_new_secret(ca_certificate, domains)
        self._encryption_key_submitted = False
        self._key_command: Optional[dict] = None

    @property
    def feed_id(self):
        return self.__feed['feed_id']

    async def run(self):
        if self.__refresh_rate:
            # Runs until stopped at the defined refresh_rate
            while self.__stop_event.is_set() is False:
                await self.__scrape()
                if self.__refresh_rate:
                    try:
                        await asyncio.wait_for(self.__stop_event.wait(), self.__refresh_rate.seconds)
                        return  # Closing
                    except asyncio.TimeoutError:
                        pass
        else:
            # Runs once and exits
            await self.__scrape()

    async def stop(self):
        self.__stop_event.set()

    async def __scrape(self):
        async with self.__semaphore:
            await self.scrape()

            throttle = self._context.scrape_throttle_seconds
            if throttle:
                # Throttle, wait several seconds before releasing the semaphore
                await self._context.wait(throttle)

    async def scrape(self):
        raise NotImplementedError('Must be implemented')

    @property
    def url(self) -> str:
        return self.__url

    @property
    def etag(self) -> Optional[str]:
        return self.__etag

    @property
    def last_update(self) -> Optional[datetime.datetime]:
        return self.__last_update

    def update(self, parameters: FeedParametersType):
        raise NotImplementedError('must implement')

    def set_update_time(self, etag: Optional[str] = None):
        self.__last_update = datetime.datetime.now(tz=pytz.UTC)
        self.__etag = etag

