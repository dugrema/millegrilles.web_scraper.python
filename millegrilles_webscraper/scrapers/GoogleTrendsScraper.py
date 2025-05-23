import asyncio
import binascii
import datetime
import json
import math
import tempfile

import aiohttp
import logging

from io import BytesIO
from typing import Optional, TypedDict

from xml.etree import ElementTree as ET

from millegrilles_messages.messages import Constantes
from millegrilles_messages.chiffrage.Mgs4 import chiffrer_document
from millegrilles_messages.messages.EnveloppeCertificat import EnveloppeCertificat
from millegrilles_messages.messages.Hachage import hacher_to_digest
from millegrilles_webscraper.Context import WebScraperContext
from millegrilles_webscraper.DataCollectorItem import DataCollectorItem, DataCollectorDict
from millegrilles_webscraper.DataStructures import AttachedFile
from millegrilles_webscraper.scrapers.WebScraper import WebScraper, FeedParametersType


class GroupData(TypedDict):
    title: Optional[str]
    approx_traffic: Optional[str]
    pub_date: Optional[int]


class ScrapedGoogleTrendsNewsItem:

    def __init__(self, group: GroupData, title: str, url: str, date: Optional[datetime.datetime]):
        self.group = group
        self.title = title
        self.url = url
        self.date = date
        self.source: Optional[str] = None
        self.picture: Optional[str] = None
        self.picture_source: Optional[str] = None


class DataCollectorClearData(TypedDict):
    title: str
    snippet: Optional[str]
    url: str
    pub_date: int
    item_source: Optional[str]
    picture_url: Optional[str]
    picture_source: Optional[str]
    # thumbnail: Optional[str]
    group: GroupData


class DataCollectorGoogleTrendsNewsItem(DataCollectorItem):

    def __init__(self, feed_id: str, scraped_item: ScrapedGoogleTrendsNewsItem):
        super().__init__(feed_id)
        self.scraped_item = scraped_item

    def get_data_id(self):
        timestamp = math.floor(self.scraped_item.date.timestamp())
        items = [self.scraped_item.title, self.scraped_item.url, timestamp]
        items_str = json.dumps(items)
        digest_value = hacher_to_digest(items_str, 'blake2s-256')
        return binascii.hexlify(digest_value).decode('utf-8')

    def produce_data(self) -> DataCollectorClearData:
        return {
            'title': self.scraped_item.title,
            'snippet': None,  # self.scraped_item.snippet
            'url': self.scraped_item.url,
            'pub_date': math.floor(self.scraped_item.date.timestamp()),
            'item_source': self.scraped_item.source,
            'picture_url': self.scraped_item.picture,
            'picture_source': self.scraped_item.picture_source,
            # 'thumbnail': None,
            'group': self.scraped_item.group,
        }


class GoogleTrendsScraper(WebScraper):

    def __init__(self, context: WebScraperContext, feed: FeedParametersType, semaphore: asyncio.BoundedSemaphore):
        super().__init__(context, feed, semaphore)
        self.__logger = logging.getLogger(f'{__name__}.{self.__class__.__name__}')

    async def process(self, input_file: tempfile.TemporaryFile, output_file: tempfile.TemporaryFile()):
        parsed_content = await self.__extract_content(input_file)
        await self.__process_content(parsed_content)

    async def __extract_content(self, temp_file: tempfile.TemporaryFile) -> list[DataCollectorGoogleTrendsNewsItem]:
        parsed_content: ET = ET.parse(temp_file)

        ns_ht = 'https://trends.google.com/trending/rss'

        root = parsed_content.getroot()

        pub_date: Optional[datetime.datetime] = None
        item_picture: Optional[str] = None
        item_picture_source: Optional[str] = None

        scraped_items_list: list[DataCollectorGoogleTrendsNewsItem] = list()

        for item in root.findall('./channel/item'):
            group: GroupData = dict()
            for child in item:
                if child.tag == '{%s}news_item' % ns_ht:
                    news_item_title: Optional[str] = None
                    news_item_url: Optional[str] = None
                    news_item_picture: Optional[str] = None
                    news_item_source: Optional[str] = None

                    for news_item in child:
                        if news_item.tag == '{%s}news_item_title' % ns_ht:
                            news_item_title = news_item.text
                        elif news_item.tag == '{%s}news_item_url' % ns_ht:
                            news_item_url = news_item.text
                        elif news_item.tag == '{%s}news_item_picture' % ns_ht:
                            news_item_picture = news_item.text
                        elif news_item.tag == '{%s}news_item_source' % ns_ht:
                            news_item_source = news_item.text

                    news_item_scraped = ScrapedGoogleTrendsNewsItem(group, news_item_title, news_item_url, pub_date)
                    news_item_scraped.source = news_item_source
                    if news_item_picture:
                        news_item_scraped.picture = news_item_picture
                        news_item_scraped.picture_source = news_item_source
                    else:
                        news_item_scraped.picture = item_picture
                        news_item_scraped.picture_source = item_picture_source

                    # scraped_items_list.append(news_item_scraped)
                    # Build the data collector item
                    data_item = DataCollectorGoogleTrendsNewsItem(self.feed_id, news_item_scraped)
                    scraped_items_list.append(data_item)

                elif child.tag == 'title':
                    group['title'] = child.text
                elif child.tag == 'pubDate':
                    pub_date = parse_date(child.text)
                    group['pub_date'] = math.floor(pub_date.timestamp())
                elif child.tag == '{%s}approx_traffic' % ns_ht:
                    group['approx_traffic'] = child.text
                elif child.tag == '{%s}picture' % ns_ht:
                    item_picture = child.text
                elif child.tag == '{%s}picture_source' % ns_ht:
                    item_picture_source = child.text
                pass

        return scraped_items_list

    async def __process_content(self, data: list[DataCollectorGoogleTrendsNewsItem]):
        # Generate ids to check which have already been produced
        data_ids = [d.get_data_id() for d in data]
        producer = await self._context.get_producer()
        response = await producer.request({"feed_id": self.feed_id, "data_ids": data_ids}, "DataCollector", "checkExistingDataIds", exchange=Constantes.SECURITE_PUBLIC)
        missing_ids = set(response.parsed['missing_ids'])

        # Filter out existing ids
        data = [d for d in data if d.get_data_id() in missing_ids]

        if len(data) == 0:
            self.__logger.debug("No changes to content since last scrape")
            # Nothing to do
            return

        self.__logger.debug("Processing %d new items" % len(data))

        if self._encryption_key_submitted is False and self._key_command is None:
            idmg = self._context.ca.idmg
            fiche_response = await producer.request({'idmg': idmg}, 'CoreTopologie', 'ficheMillegrille', exchange=Constantes.SECURITE_PUBLIC)
            encryption_keys = fiche_response.parsed['chiffrage']
            certs = [EnveloppeCertificat.from_pem('\n'.join(c)) for c in encryption_keys]
            encrypted_keys = self._encryption_key.produce_keymaster_content(certs)
            key_command, _message_id = self._context.formatteur.signer_message(
                Constantes.KIND_COMMANDE, encrypted_keys, 'MaitreDesCles', action='ajouterCleDomaines')
            self._key_command = key_command

        # Get thumbnails for all remaining items
        thumbnail_urls = set([d.scraped_item.picture for d in data])
        thumbnail_dict: dict[str, AttachedFile] = dict()
        async with aiohttp.ClientSession() as session:
            for thumbnail_url in thumbnail_urls:
                async with session.get(thumbnail_url) as response:
                    if response.status == 200:
                        content_bytes = await response.content.read()
                        # content_base64 = binascii.b2a_base64(content_bytes, newline=False)
                        # content_base64 = content_base64.decode('utf-8').replace('=', '')
                        # thumbnail_dict[thumbnail_url] = content_base64
                    else:
                        self.__logger.warning("Error loading thumbnail (%s) at %s" % (response.status, thumbnail_url))
                        await asyncio.sleep(0.5)
                        continue

                content_bytes_io = BytesIO(content_bytes)
                thumbnail_result: AttachedFile = await self._context.file_handler.encrypt_upload_file(self._encryption_key.secret_key, content_bytes_io)
                thumbnail_result['cle_id'] = self._encryption_key.key_id
                thumbnail_dict[thumbnail_url] = thumbnail_result

        # Encrypt content and produce DataCollector item
        for item in data:
            item_data = item.produce_data()

            encrypted_data = chiffrer_document(self._encryption_key.secret_key, self._encryption_key.key_id, item_data)
            # Rename the data_chiffre field to new standard ciphertext_base64
            encrypted_data['ciphertext_base64'] = encrypted_data['data_chiffre']
            del encrypted_data['data_chiffre']
            data_collector_dict: DataCollectorDict = {
                'data_id': item.get_data_id(),
                'feed_id': self.feed_id,
                'pub_date': item_data['pub_date'],
                'encrypted_data': encrypted_data,
            }

            # Inject thumbnail if present
            picture_url = item_data['picture_url']
            thumbnail = thumbnail_dict.get(picture_url)
            if thumbnail:
                cle_id: str = thumbnail['cle_id'] or self._encryption_key.key_id
                data_collector_dict['files'] = [
                    {
                        'fuuid': thumbnail['fuuid'],
                        'decryption': {'cle_id': cle_id, 'nonce': thumbnail['nonce'], 'format': thumbnail['format']}
                    }
                ]

            # Emit item for saving in the DataCollector domain
            attachments: Optional[dict[str, dict]] = None
            if self._key_command:
                attachments = {'key': self._key_command}

            response = await producer.command(data_collector_dict, "DataCollector", "saveDataItem",
                                              exchange=Constantes.SECURITE_PUBLIC, attachments=attachments)

            if self._encryption_key_submitted is False and response.parsed['ok'] is True:
                # Key saved successfully
                self._key_command = None
                self._encryption_key_submitted = True

            pass

        pass


def parse_date(date_str: str) -> datetime.datetime:
    return datetime.datetime.strptime(date_str, '%a, %d %b %Y %H:%M:%S %z')
