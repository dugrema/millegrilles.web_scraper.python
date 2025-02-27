import asyncio

from millegrilles_webscraper.Configuration import WebScraperConfiguration
from millegrilles_webscraper.Context import WebScraperContext
from millegrilles_webscraper.scrapers.CBCScraper import CBCScraper


async def main():
    config = WebScraperConfiguration.load()
    context = WebScraperContext(config)
    semaphore = asyncio.BoundedSemaphore(1)

    # cbc_scraper = CBCScraper(context, 'https://www.cbc.ca/webfeed/rss/rss-topstories', semaphore)
    cbc_scraper = CBCScraper(context, 'https://www.cbc.ca/webfeed/rss/rss-canada', semaphore)

    await cbc_scraper.run()


if __name__ == '__main__':
    asyncio.run(main())
