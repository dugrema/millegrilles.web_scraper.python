import asyncio

from millegrilles_webscraper.Configuration import WebScraperConfiguration
from millegrilles_webscraper.Context import WebScraperContext
from millegrilles_webscraper.scrapers.GoogleTrendsScraper import GoogleTrendsScraper


async def main():
    config = WebScraperConfiguration.load()
    context = WebScraperContext(config)
    semaphore = asyncio.BoundedSemaphore(1)

    google_scraper = GoogleTrendsScraper(context, 'https://trends.google.com/trending/rss?geo=US', semaphore)
    await google_scraper.run()


if __name__ == '__main__':
    asyncio.run(main())
