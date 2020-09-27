from alpha_vantage import async_support as vasy
import asyncio
import aiohttp
import aiomoex
import pandas as pd
from typing import List, Union, Tuple
import yfinance
from alpha_vantage.async_support.timeseries import TimeSeries
from settingscomponent.loader import SETTINGS, loop, SEQURITIES
from datetime import datetime, timedelta
from dateutil import relativedelta

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

columns = ("BOARDID", "TRADEDATE", "CLOSE", "VOLUME", "VALUE", "YIELD"),

class moex_connector:
    def __init__(self, secondary: List[dict], market):
        self.secondary = secondary
        self.market = market

    @staticmethod
    async def _get_board_hist(session, name, board, market, start, end):
        return await aiomoex.get_board_history(session, security=name, board=board,
                                               market=market, columns=None, start=start, end=end)
    @staticmethod
    async def _init_issclient(session, request_url, args= None):

        iss = aiomoex.ISSClient(session, request_url, args)
        data = await iss.get()
        return data


    async def _get_coupons(self, session, name):

        request_url = "https://iss.moex.com/iss/statistics/engines/stock/markets/bonds/bondization/" + name + '.json'
        return await self._init_issclient(session, request_url)

    async def _get_dividends(self, session, name):

        request_url = "http://iss.moex.com/iss/securities/" + name + "/dividends.json"
        return await self._init_issclient(session, request_url)

    async def get_board_hists(self, start=None, end=None):
        format = '%Y-%m-%d'
        if start is None:
            now = datetime.now()
            delta = relativedelta.relativedelta(years=5)
            calc_start = now - delta
            start = calc_start.strftime(format)
        if end is None:
            now = datetime.now()
            end = now.strftime(format)

        async with aiohttp.ClientSession() as session:

            tasks = [self._get_board_hist(session, second['name'],
                    second['board'], self.market, start, end)
                     for second in self.secondary]

            data_list = await asyncio.gather(*tasks)
        return data_list

    async def get_dividends(self):
        if self.market == 'shares':
            async with aiohttp.ClientSession() as session:
                async def skip_dividend(secondary):
                    """
                    Skip downloading dividends from moex if skip_d flag is set
                    Args:
                        secondary: symbol from configuration
                    Returns:
                        object with empty dividends, for now
                    """

                    async def return_empty():
                        """
                        This method is a placeholder for future loading of custom dividends
                        Returns:
                            empty for now
                        """
                        return {'dividends': []}

                    if 'skip_d' not in secondary:
                        dividends = await self._get_dividends(session, secondary['name'])
                    else:
                        dividends = await return_empty()

                    return dividends

                tasks = [skip_dividend(second)
                         for second in self.secondary]
                data_list = await asyncio.gather(*tasks)
                return data_list
        else:
            return None

    async def get_coupons(self):
        if self.market == 'bonds':
            async with aiohttp.ClientSession() as session:
                tasks = [self._get_coupons(session, second['name'])
                         for second in self.secondary]
                data_list = await asyncio.gather(*tasks)
                return data_list
        else:
            return None




