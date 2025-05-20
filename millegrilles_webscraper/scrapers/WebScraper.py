import logging

import aiohttp
import asyncio
import datetime
import tempfile
import pathlib

from typing import Optional, TypedDict

import pytz

from millegrilles_messages.chiffrage.EncryptionKey import generate_new_secret
from millegrilles_webscraper.Context import WebScraperContext

CHUNK_SIZE = 1024 * 64


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
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__stop_event = asyncio.Event()
        self._context = context
        self.__semaphore = semaphore
        self.__feed = feed

        self.__url = feed['decrypted_feed_information']['url']
        self.__refresh_rate = None
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

        # Finish loading all parameters
        self.update(feed)

    @property
    def feed_id(self):
        return self.__feed['feed_id']

    def update_poll_rate(self, rate: Optional[datetime.timedelta]):
        self.__refresh_rate = rate

    async def run(self):
        if self.__refresh_rate:
            # Runs until stopped at the defined refresh_rate
            while self.__stop_event.is_set() is False:
                try:
                    await self.__scrape()
                except asyncio.TimeoutError:
                    self.__logger.warning(f"Timeout while processing {self.url}")
                except aiohttp.ClientResponseError as cre:
                    if cre.status == 429:
                        self.__logger.warning("Received HTTP 429 on %s, sleeping for a while" % self.url)
                        try:
                            if self.__refresh_rate.seconds < 3600:
                                await asyncio.wait_for(self.__stop_event.wait(), 3600)
                                return  # Closing
                        except asyncio.TimeoutError:
                            continue  # Retry
                    else:
                        raise cre
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
            self.__logger.debug(f"Scraping START on {self.url}")

            try:
                with tempfile.TemporaryFile('wb+') as temp_input_file:
                    len_file = await self.get_content(temp_input_file)
                    if len_file > 0:
                        self.__logger.debug(f"Scraped {len_file} bytes, processing latest {self.url}")
                        temp_input_file.seek(0)  # Reposition file pointer to start processing
                        with tempfile.TemporaryFile('wb+') as temp_output_file:
                            await self.process(temp_input_file, temp_output_file)
                    else:
                        self.__logger.debug(f"No content found for {self.url}, skipping")

                self.__logger.debug(f"Scraping DONE on {self.url}")
            except asyncio.TimeoutError:
                self.__logger.warning(f"Timeout when fetching web content on {self.url}")

            throttle = self._context.scrape_throttle_seconds
            if throttle:
                # Throttle, wait several seconds before releasing the semaphore
                self.__logger.debug(f"Throttling after {self.url}")
                await self._context.wait(throttle)

    async def get_content(self, tmp_file: tempfile.TemporaryFile) -> int:
        len_file = 0

        if self.url.startswith("file://"):
            # Process local file
            local_path_str = self.url[len("file://"):]
            local_filename = pathlib.Path(local_path_str)
            with open(local_filename, 'rb') as fp:
                while True:
                    chunk = fp.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    tmp_file.write(chunk)
                    len_file += len(chunk)
        else:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.url) as response:
                    response.raise_for_status()
                    async for chunk in response.content.iter_chunked(CHUNK_SIZE):
                        await asyncio.to_thread(tmp_file.write, chunk)
                        len_file += len(chunk)

        return len_file

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
        poll_rate_update = parameters['poll_rate']
        if poll_rate_update:
            if poll_rate_update < 30:
                # Minimum polling of 30 seconds
                self.update_poll_rate(datetime.timedelta(seconds=30))
            else:
                self.update_poll_rate(datetime.timedelta(seconds=poll_rate_update))
        else:
            self.update_poll_rate(None)

    def set_update_time(self, etag: Optional[str] = None):
        self.__last_update = datetime.datetime.now(tz=pytz.UTC)
        self.__etag = etag

    async def process(self, temp_input_file: tempfile.TemporaryFile, temp_output_file: tempfile.TemporaryFile()):
        raise NotImplementedError('Must be implemented')
