import asyncio
from asyncio import TaskGroup

from millegrilles_messages.bus.PikaConnector import MilleGrillesPikaConnector
from millegrilles_webscraper.Configuration import WebScraperConfiguration
from millegrilles_webscraper.Context import WebScraperContext
from millegrilles_webscraper.scrapers.AttachedFileHelper import AttachedFileHelper
from millegrilles_webscraper.scrapers.WebCustomPythonScraper import WebCustomPythonScraper


CUSTOM_PROCESS = """
from io import BytesIO

import aiohttp
import asyncio
import datetime
import tempfile

from typing import Optional
from xml.etree import ElementTree as ET

from millegrilles_messages.chiffrage.EncryptionKey import EncryptionKey
from millegrilles_messages.messages.Hachage import hacher
from millegrilles_webscraper.Context import WebScraperContext
from millegrilles_webscraper.DataStructures import CustomProcessOutput, AttachedFile, AttachedFileCorrelation

NAMESPACE_HT = 'https://trends.google.com/trending/rss'

class PictureInfo(AttachedFileCorrelation):

    def __init__(self, url: str):
        correlation = hacher(url, 'blake2s-256', 'base64')[1:]  # Remove multibase marker
        super().__init__(correlation)
        self.url = url

    def map_key(self) -> Optional[str]:
        return self.url


def __extract_data(input_file: tempfile.TemporaryFile) -> (datetime.datetime, datetime.datetime, dict[str, PictureInfo]):
    # Parse the input file
    parsed_content: ET = ET.parse(input_file)
    input_file.seek(0)  # Reset input file position

    # Extract all pictures
    pub_date_start: Optional[datetime.datetime] = None
    pub_date_end: Optional[datetime.datetime] = None
    picture_urls: dict[str, PictureInfo] = dict()

    root = parsed_content.getroot()
    for item in root.findall('./channel/item'):
        pub_date_item_elem = item.find('pubDate')
        if pub_date_item_elem is not None:
            pub_date_item_str = pub_date_item_elem.text
            pub_date = parse_date(pub_date_item_str)
            print("PubDate %s = %s" % (pub_date_item_str, pub_date))
            if pub_date_start is None or pub_date_start > pub_date:
                pub_date_start = pub_date
            if pub_date_end is None or pub_date_end < pub_date:
                pub_date_end = pub_date

        for news_item in item.findall('./{%s}news_item' % NAMESPACE_HT):
            picture_url = news_item.find('./{%s}news_item_picture' % NAMESPACE_HT)
            if picture_url is None:
                picture_url = item.find('{%s}picture' % NAMESPACE_HT)
    
            if picture_url is not None:
                picture_url_text = picture_url.text
                if picture_url_text is not None:
                    # Hash the url with blake2s un base64, this will be used to check if the picture has already been loaded
                    picture_info = PictureInfo(picture_url_text)
                    picture_urls[picture_info.correlation] = picture_info

    return pub_date_start, pub_date_end, picture_urls

async def __verify_image_digests(context: WebScraperContext, picture_urls: dict[str, PictureInfo]):
    # Check with DataCollector to determine which pictures have already been uploaded to filehost
    picture_digests = [d for d in picture_urls.keys()]
    print("Check if digests exist: %s" % picture_digests)

    query = {"correlations": picture_digests}
    producer = await context.get_producer()
    response = await producer.request(query, "DataCollector", "getFuuidsVolatile", exchange="1.public")
    response_parsed = response.parsed
    if response_parsed.get('ok') is True:
        existing_files = response_parsed['files']
        print("Reusing volatile files: %s" % existing_files)
        for existing_file in existing_files:
            # Map existing
            attached_file = picture_urls[existing_file['correlation']]
            attached_file.map_volatile(existing_file)
    else:
        print("Volatil check error response: %s" % response_parsed)

async def __download_save_pictures(context: WebScraperContext, encryption_key: EncryptionKey, picture_urls: dict[str, PictureInfo]):
    for picture_info in picture_urls.values():
        if picture_info.fuuid is None:
            async with aiohttp.ClientSession() as session:
                picture_url = picture_info.url
                print("Downloading thumbnail %s" % picture_url)
                async with session.get(picture_url) as response:
                    if response.status == 200:
                        content_bytes = await response.content.read()
                        content_bytes_io = BytesIO(content_bytes)
                        attached_file: AttachedFile = await context.file_handler.encrypt_upload_file(
                            encryption_key.secret_key, content_bytes_io)

                        # Inject file information into picture_info
                        picture_info.fuuid = attached_file['fuuid']
                        picture_info.format = attached_file['format']
                        picture_info.compression = attached_file.get('compression')
                        picture_info.nonce = attached_file.get('nonce')
                        picture_info.cle_id = encryption_key.key_id
                    else:
                        print("Error loading thumbnail (%s) at %s" % (response.status, picture_url))
                        await asyncio.sleep(0.5)
                        continue
            pass

    return None

def parse_date(date_str: str) -> datetime.datetime:
    return datetime.datetime.strptime(date_str, '%a, %d %b %Y %H:%M:%S %z')

# Note: Call await process(..) from exec globals
async def process(context: WebScraperContext, encryption_key: EncryptionKey,
                  input_file: tempfile.TemporaryFile) -> Optional[CustomProcessOutput]:
    output = CustomProcessOutput()

    # Extract picture URLs from input
    output.pub_date_start, output.pub_date_end, attached_files = await asyncio.to_thread(__extract_data, input_file)
    # Remove already downloaded pictures (checking with DataCollector domain)
    await __verify_image_digests(context, attached_files)
    # Download pictures and save to filehost
    await __download_save_pictures(context, encryption_key, attached_files)
    output.files = [f for f in attached_files.values()]

    return output
"""


async def run_scrape_test(context: WebScraperContext):
    semaphore = asyncio.BoundedSemaphore(1)

    feed = {
        'feed_id': 'Test',
        'decrypted_feed_information': {
            'url': 'https://trends.google.com/trending/rss?geo=US',
            'custom_code': CUSTOM_PROCESS,
        },
        # 'decrypted_feed_information': {'url': 'file:///home/mathieu/Downloads/rss_googletrends_us_20250411_0816.xml'},
        'poll_rate': 120,
    }

    google_scraper = WebCustomPythonScraper(context, feed, semaphore)
    await google_scraper.run()
    # with open('/home/mathieu/Downloads/rss_US_20250227_1250.xml', 'rb') as file:
    #     content = file.read()
    #
    # item_list = await google_scraper.extract_content(content)
    # result = await google_scraper.process_content(item_list)

    context.stop()


async def main():
    config = WebScraperConfiguration.load()
    context = WebScraperContext(config)
    bus_connector = MilleGrillesPikaConnector(context)
    context.bus_connector = bus_connector
    attached_file_helper = AttachedFileHelper(context)

    # Additional wiring
    context.file_handler = attached_file_helper

    # Create tasks
    async with TaskGroup() as group:
        group.create_task(context.run())
        group.create_task(bus_connector.run())
        group.create_task(run_scrape_test(context))
        group.create_task(attached_file_helper.run())


if __name__ == '__main__':
    asyncio.run(main())
