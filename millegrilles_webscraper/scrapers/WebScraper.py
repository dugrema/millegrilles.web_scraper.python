import asyncio
import datetime

from typing import Optional

import pytz

from millegrilles_webscraper.Context import WebScraperContext


class WebScraper:

    def __init__(self, context: WebScraperContext, url: str, semaphore: asyncio.BoundedSemaphore, refresh_rate: Optional[datetime.timedelta] = None):
        self.__context = context
        self.__url = url
        self.__semaphore = semaphore
        self.__refresh_rate = refresh_rate

        self.__stop_event = asyncio.Event()

        self.__last_update: Optional[datetime.datetime] = None
        self.__etag: Optional[str] = None

    async def run(self):
        if self.__refresh_rate:
            # Runs until stopped at the defined refresh_rate
            while self.__stop_event.is_set() is False:
                await self.__scrape()
                if self.__refresh_rate:
                    try:
                        await asyncio.wait_for(self.__stop_event.wait(), self.__refresh_rate.seconds)
                        return  # Closing
                    except asyncio.TimeoutError:
                        pass
        else:
            # Runs once and exits
            await self.__scrape()

    async def __scrape(self):
        async with self.__semaphore:
            await self.scrape()

            throttle = self.__context.scrape_throttle_seconds
            if throttle:
                # Throttle, wait several seconds before releasing the semaphore
                await self.__context.wait(throttle)

    async def scrape(self):
        raise NotImplementedError('Must be implemented')

    @property
    def url(self) -> str:
        return self.__url

    @property
    def etag(self) -> Optional[str]:
        return self.__etag

    @property
    def last_update(self) -> Optional[datetime.datetime]:
        return self.__last_update

    def set_update_time(self, etag: Optional[str] = None):
        self.__last_update = datetime.datetime.now(tz=pytz.UTC)
        self.__etag = etag

