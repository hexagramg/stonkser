from alpha_vantage import async_support as vasy
import asyncio
import aiohttp
from typing import List, Union, Tuple
import yfinance
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
    """
    connector to the Alpha Vantage API library wrapper
    
    Attributes:
        symbols: list of symbols to get info for
        ts: time series object from the wrapper with the key
    """
    def __init__(self, symbols: List[str]):
        self.ts = TimeSeries(key=API)
        self.symbols = symbols

    async def _get_quote(self, symbol: str):
        """inner function for getting exact quote
        Args:
            symbol (str): the name of the symbol to get quote from AV api
        Returns:
            returns dict
            """
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


class YFinanceConnector:
    def __init__(self, symbols: List[str]):
        self.symbols = symbols
        self.tickers = yfinance.Tickers(symbols)

    def get_history(self, period: str = '5y', interval: str = '1d', group_by='ticker') -> List[dict]:
        """
        funcion gets from yfinance pandas dataframe with downloaded data
        Returns:
            array of dicts, be aware of TIMESTAMPS
        """
        self.history = self.tickers.history(period=period, interval=interval, group_by=group_by)
        data_in_dict = []
        for symbol in self.symbols:
            buffer = self.history[symbol].to_dict('index')
            data_in_dict.append(buffer)

        return data_in_dict
