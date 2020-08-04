from alpha_vantage import async_support as vasy
import asyncio
import aiohttp
from typing import List, Union, Tuple

from alpha_vantage.async_support.timeseries import TimeSeries
from settingscomponent.loader import SETTINGS, loop
API = SETTINGS['VANTAGE']
LIMIT = 5
BLOCKER = asyncio.locks.Semaphore(LIMIT)


def release():
    BLOCKER.release()


async def limiter():
    await BLOCKER.acquire()
    loop.call_later(60, release)


class VantageConnector:

    def __init__(self, symbols: List[str]):
        self.ts = TimeSeries(key=API)
        self.symbols = symbols

    async def _get_quote(self, symbol: str):
        await limiter()
        data, _ = await self.ts.get_quote_endpoint(symbol)
        return data

    async def get_quotes(self) -> Tuple[List[dict], List[str]]:
        tasks = [self._get_quote(symbol) for symbol in self.symbols]
        group = await asyncio.gather(*tasks)
        return group, self.symbols

    async def daily_adjusted(self, size='compact') -> Tuple[List[dict], List[str]]:
        async def get_data(symbol):
            await limiter()
            data, _ = await self.ts.get_daily_adjusted(symbol, outputsize=size)
            return data

        tasks = [get_data(symbol) for symbol in self.symbols]
        group: List[dict] = await asyncio.gather(*tasks)
        return group, self.symbols

    def __del__(self):
        asyncio.ensure_future(self.ts.close())

