import asyncio

from millegrilles_webscraper.Configuration import WebScraperConfiguration
from millegrilles_webscraper.Context import WebScraperContext
from millegrilles_webscraper.scrapers.FoxNewsScraper import FoxNewsScraper


async def main():
    config = WebScraperConfiguration.load()
    context = WebScraperContext(config)
    semaphore = asyncio.BoundedSemaphore(1)

    cbc_scraper = FoxNewsScraper(context, 'https://moxie.foxnews.com/google-publisher/latest.xml', semaphore)

    await cbc_scraper.run()


if __name__ == '__main__':
    asyncio.run(main())
