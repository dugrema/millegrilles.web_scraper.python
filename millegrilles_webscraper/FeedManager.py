import asyncio
import base64
import binascii
import logging

from asyncio import TaskGroup
from typing import Optional, TypedDict

from millegrilles_messages.chiffrage.DechiffrageUtils import dechiffrer_reponse, dechiffrer_document_secrete
from millegrilles_messages.messages import Constantes
from millegrilles_webscraper.Context import WebScraperContext
from millegrilles_webscraper.scrapers import WebScraper
from millegrilles_webscraper.scrapers.GoogleTrendsScraper import GoogleTrendsScraper
from millegrilles_webscraper.scrapers.WebCustomPythonScraper import WebCustomPythonScraper
from millegrilles_webscraper.scrapers.WebScraper import FeedParametersType, FeedInformation


class DecryptedKeyDict(TypedDict):
    cle_id: str
    cle_secrete_base64: str


class DecryptedKey:

    def __init__(self, key_id: str, secret_key: bytes):
        self.key_id = key_id
        self.secret_key = secret_key


class FeedManager:

    def __init__(self, context: WebScraperContext):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__context = context

        # Feed semaphore to limit the number of scrapers running at the same time
        self.__feed_semaphore = asyncio.BoundedSemaphore(1)

        self.__scapers: dict[str, WebScraper] = dict()
        self.__group: Optional[TaskGroup] = None

    async def run(self):
        async with TaskGroup() as group:
            self.__group = group
            group.create_task(self.__maintain_scraper_thread())

    async def __maintain_scraper_thread(self):
        while self.__context.stopping is False:
            try:
                await self.maintain_scraper_list()
            except asyncio.TimeoutError:
                self.__logger.warning("Timeout refreshing feeds, retry in 15 seconds")
                await self.__context.wait(15)
            else:
                await self.__context.wait(300)

    async def maintain_scraper_list(self):
        producer = await self.__context.get_producer()
        self.__logger.info("Refreshing feeds")

        # Load scraper configuration from DataCollector
        response = await producer.request(dict(), 'DataCollector', 'getFeedsForScraper', exchange=Constantes.SECURITE_PUBLIC)

        if response.parsed.get('ok') is not True:
            raise Exception(response.parsed.get('err'))

        feeds: list[FeedParametersType] = response.parsed['feeds']
        keys = response.parsed['keys']

        if len(feeds) == 0:
            self.__logger.info("No configured/active scraper")
            return
        elif keys is None:
            raise ValueError('No decryption keys were received')

        # Decrypt the keys message
        decrypted_key_message = dechiffrer_reponse(self.__context.signing_key, keys)
        decrypted_key_map: dict[str, bytes] = dict()
        key: DecryptedKeyDict
        for key in decrypted_key_message['cles']:
            key_id = key['cle_id']
            secret_key_base64 = key['cle_secrete_base64']
            secret_key_base64 += "=" * ((4 - len(secret_key_base64) % 4) % 4)  # Padding
            key_bytes: bytes = binascii.a2b_base64(secret_key_base64)
            decrypted_key_map[key_id] = key_bytes

        # Decrypt feed configuration
        unchanged_scraper_feed_ids = set(self.__scapers.keys())
        for feed in feeds:
            feed_id = feed['feed_id']
            try:
                unchanged_scraper_feed_ids.remove(feed_id)
            except KeyError:
                pass

            encrypted_info = feed['encrypted_feed_information']
            key: bytes = decrypted_key_map[encrypted_info['cle_id']]
            cleartext_content: FeedInformation = dechiffrer_document_secrete(key, encrypted_info)
            feed['decrypted_feed_information'] = cleartext_content

            existing_scraper: WebScraper = self.__scapers.get(feed_id)
            if existing_scraper:
                existing_scraper.update(feed)
            else:
                # Create and start the scraper
                scraper = self.create_scraper(feed)
                self.__scapers[feed_id] = scraper
                self.__group.create_task(scraper.run())

        for removed_scraper_id in unchanged_scraper_feed_ids:
            # This scraper was removed (deleted on inactive)
            scraper: WebScraper = self.__scapers.get(removed_scraper_id)
            if scraper:
                self.__logger.info("Stopping scraper id: %s" % removed_scraper_id)
                del self.__scapers[removed_scraper_id]
                await scraper.stop()

        pass

    def create_scraper(self, feed: FeedParametersType) -> WebScraper:
        feed_type = feed['feed_type']
        if feed_type == 'web.google_trends.news':
            return GoogleTrendsScraper(self.__context, feed, self.__feed_semaphore)
        elif feed_type == 'web.scraper.python_custom':
            return WebCustomPythonScraper(self.__context, feed, self.__feed_semaphore)
        else:
            raise NotImplementedError('Unsupported feed type: %s' % feed_type)
