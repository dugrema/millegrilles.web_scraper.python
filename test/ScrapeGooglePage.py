import asyncio
from asyncio import TaskGroup

from millegrilles_messages.bus.PikaConnector import MilleGrillesPikaConnector
from millegrilles_webscraper.Configuration import WebScraperConfiguration
from millegrilles_webscraper.Context import WebScraperContext
from millegrilles_webscraper.scrapers.GoogleTrendsScraper import GoogleTrendsScraper


async def run_scrape_test(context: WebScraperContext):
    semaphore = asyncio.BoundedSemaphore(1)

    feed = {
        'feed_id': 'Test',
        'decrypted_feed_information': {'url': 'https://trends.google.com/trending/rss?geo=US'},
    }

    google_scraper = GoogleTrendsScraper(context, feed, semaphore)
    # await google_scraper.run()
    with open('/home/mathieu/Downloads/rss_US_20250227_1250.xml', 'rb') as file:
        content = file.read()

    item_list = await google_scraper.extract_content(content)
    result = await google_scraper.process_content(item_list)

    context.stop()


async def main():
    config = WebScraperConfiguration.load()
    context = WebScraperContext(config)
    bus_connector = MilleGrillesPikaConnector(context)
    context.bus_connector = bus_connector

    # Create tasks
    async with TaskGroup() as group:
        group.create_task(context.run())
        group.create_task(bus_connector.run())
        group.create_task(run_scrape_test(context))


if __name__ == '__main__':
    asyncio.run(main())
