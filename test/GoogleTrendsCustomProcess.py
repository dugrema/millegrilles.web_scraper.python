import asyncio
import datetime
import tempfile

from typing import Optional
from xml.etree import ElementTree as ET

from millegrilles_messages.chiffrage.EncryptionKey import EncryptionKey
from millegrilles_messages.messages.Hachage import hacher
from millegrilles_webscraper.Context import WebScraperContext
from millegrilles_webscraper.DataStructures import DataCollectorTransaction, CustomProcessOutput, AttachedFile

NAMESPACE_HT = 'https://trends.google.com/trending/rss'

def __extract_data(input_file: tempfile.TemporaryFile) -> (datetime.datetime, datetime.datetime, dict[str, str]):
    # Parse the input file
    parsed_content: ET = ET.parse(input_file)
    input_file.seek(0)  # Reset input file position

    # Extract all pictures
    pub_date_start: Optional[datetime.datetime] = None
    pub_date_end: Optional[datetime.datetime] = None
    picture_urls: dict[str, str] = dict()

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

        picture_url = item.find('{%s}news_item/{%s}news_item_picturea' % (NAMESPACE_HT, NAMESPACE_HT))
        if picture_url is None:
            picture_url = item.find('{%s}picture' % NAMESPACE_HT)

        if picture_url is not None:
            # Hash the url with blake2s un base64, this will be used to check if the picture has already been loaded
            picture_url_digest = hacher(picture_url.text, 'blake2s-256', 'base64')[1:]  # Remove multibase marker
            picture_urls[picture_url_digest] = picture_url.text

    return pub_date_start, pub_date_end, picture_urls

async def __verify_image_digests(context: WebScraperContext, picture_urls: dict[str, str]):
    # Check with DataCollector to determine which pictures have already been uploaded to filehost
    picture_digests = [d for d in picture_urls.keys()]
    print("Check if digests exist: %s" % picture_digests)

    existing = []  # TODO - query to get existing digests

    for existing_digest in existing:
        del picture_urls[existing_digest]

async def __download_save_pictures(context: WebScraperContext, encryption_key: EncryptionKey, picture_urls: dict[str, str]) -> list[AttachedFile]:
    attached_files: list[AttachedFile] = list()

    for picture_url in picture_urls.values():
        pass

    return attached_files

def parse_date(date_str: str) -> datetime.datetime:
    return datetime.datetime.strptime(date_str, '%a, %d %b %Y %H:%M:%S %z')

# Note: Call await process(..) from exec globals
async def process(context: WebScraperContext, encryption_key: EncryptionKey,
                  input_file: tempfile.TemporaryFile) -> Optional[CustomProcessOutput]:
    output = CustomProcessOutput()

    # Extract picture URLs from input
    output.pub_date_start, output.pub_date_end, picture_urls = await asyncio.to_thread(__extract_data, input_file)
    # Remove already downloaded pictures (checking with DataCollector domain)
    await __verify_image_digests(context, picture_urls)
    # Download pictures and save to filehost
    output.files = await __download_save_pictures(context, encryption_key, picture_urls)

    return output
