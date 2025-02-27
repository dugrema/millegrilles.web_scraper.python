import asyncio

from millegrilles_webscraper.Configuration import WebScraperConfiguration
from millegrilles_webscraper.Context import WebScraperContext
from millegrilles_webscraper.scrapers.HtmlScraper import HtmlScraper


async def main():
    config = WebScraperConfiguration.load()
    context = WebScraperContext(config)
    semaphore = asyncio.BoundedSemaphore(1)

    html_scraper = HtmlScraper(context, 'https://www.dailymail.co.uk/sport/othersports/article-14443977/Joy-Taylor-sidelined-Fox-Sports-sex-lawsuit-Speak.html', semaphore)
    await html_scraper.run()


if __name__ == '__main__':
    asyncio.run(main())
