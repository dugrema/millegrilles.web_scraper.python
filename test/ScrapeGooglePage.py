import asyncio

from millegrilles_webscraper.Configuration import WebScraperConfiguration
from millegrilles_webscraper.Context import WebScraperContext
from millegrilles_webscraper.scrapers.GoogleTrendsScraper import GoogleTrendsScraper


async def main():
    config = WebScraperConfiguration.load()
    context = WebScraperContext(config)
    semaphore = asyncio.BoundedSemaphore(1)

    google_scraper = GoogleTrendsScraper(context, 'https://trends.google.com/trending/rss?geo=US', semaphore)
    # await google_scraper.run()
    with open('/home/mathieu/Downloads/rss_US_20250227_1250.xml', 'rb') as file:
        content = file.read()

    await google_scraper.extract_content(content)


if __name__ == '__main__':
    asyncio.run(main())
