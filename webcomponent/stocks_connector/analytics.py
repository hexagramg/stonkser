from typing import List, Tuple, Union, Callable
import asyncio as asy
import dateutil.parser as parser
import datetime as dt
from .main import *
from storagecomponent.connector import *
from functools import partial
from settingscomponent.loader import loop
COMPACT_DELTA = dt.timedelta(days=99)
TIMEZONE = dt.timezone(dt.timedelta(hours=3))

STATS = {
    'weekly': {
        'year': 52, #weeks delay for yearly stats,
        'season': 16, #weeks delay for seasonal stats
        'month': 4, #weeks delay for month stats
        },
    'daily': {
        'week': 5, #days delay for week stats
    }
}


class DataGetter:

    @staticmethod
    async def get_symbols(symbols: List[str]):
        connector = VantageConnector(symbols)
        data, _ = await connector.get_quotes()
        return data, symbols

    @staticmethod
    async def get_new_data(symbols: List[str], date: Union[datetime, None] = None) -> List[dict]:
        now = dt.datetime.now()  # tz=TIMEZONE)
        connector = VantageConnector(symbols)

        async def daily_full():
            _data, _symbols = await connector.daily_adjusted('full')
            return _data, _symbols

        if date is None or date < now - COMPACT_DELTA:
            data, _ = await daily_full()
        else:
            data, _ = await connector.daily_adjusted()

        return data


class DataAnalysis:
    @classmethod
    async def create(cls, secondary_data: List[dict]):
        self = cls(secondary_data)
        await self.preload()
        await self.preload_data_pipe()
        return self

    def __init__(self, secondary_data: List[dict]): #do not use this
        self.symbols = [sec_d['name'] for sec_d in secondary_data]
        self.secondary = secondary_data
        self.stats = {}

    async def preload(self):

        #tasks = [
        #    find_sequrity(symbol) for symbol in self.symbols
        #]
        #results = await asy.gather(*tasks)
        #indices = []
        #for ind, (result, symbol) in enumerate(zip(results, self.symbols)):
        #    if results is None:
        #        indices.append(ind)
        #        todo_symbols.append(symbol)
        todo_symbols = self.symbols

        data, _ = await DataGetter.get_symbols(todo_symbols)

        insert_tasks = [insert_sequrity(sequrity) for sequrity in data]
        insertions = await asy.gather(*insert_tasks)

    #   for index, sequrity in zip(indices, insertions):
    #       results[index] = sequrity

        self.sequrities = insertions

    @staticmethod
    async def data_load_pipe(date_func: Callable, data_download_func: Callable,
                             data_save_func: Callable, filter_func: Callable):
        date = await date_func()
        if date is not None:
            date = date['date']
        new_data = await data_download_func(date)
        _ = await data_save_func(new_data[0]) #CAN FALL IF NOTHINGH DOWNLOADED
        results = await filter_func()

        return results

    async def preload_data_pipe(self):
        async def preload_with_filter(_filter_func: Callable):
            tasks = []

            for symbol in self.symbols:
                date_func = partial(find_last_record, symbol)
                data_download_func = partial(DataGetter.get_new_data, [symbol])
                data_save_func = partial(insert_daily_vantage, symbol)
                filter_func = partial(_filter_func, symbol)
                pipeline = self.data_load_pipe(date_func, data_download_func,
                                               data_save_func, filter_func)
                tasks.append(pipeline)

            results: List[List[dict]] = await asy.gather(*tasks)
            return results

        self.data_weekly = await preload_with_filter(aggreagate_daily_month)
        self.data_daily = await preload_with_filter(aggregate_daily)

    async def calc_stats(self):
        def ret_dict(weekly_ts, daily_ts) -> dict:
            result = {}
            for key, stat in STATS['weekly'].items():
                result[key] = weekly_ts[stat - 1]

            for key, stat in STATS['daily'].items():
                result[key] = daily_ts[stat - 1]

            return result

        def extract_difference(chosen, string, price):
            return chosen[string]['close'] - price

        def calc_relative(before, offset):
            return ((before+offset)/before - 1) * 100

        def calc_difference():
            for index, _sec_data in enumerate(self.secondary):
                weekly_ts = self.data_weekly[index]
                daily_ts = self.data_daily[index]
                chosen_from_ts = ret_dict(weekly_ts, daily_ts)
                buy = float(_sec_data['buy'])
                price = float(self.sequrities[index]['price'])
                name = _sec_data['name']

                if name not in self.stats:
                    self.stats[name] = {}
                self.stats[name]['year'] = extract_difference(chosen_from_ts, 'year', price)
                self.stats[name]['season'] = extract_difference(chosen_from_ts, 'season', price)
                self.stats[name]['month'] = extract_difference(chosen_from_ts, 'month', price)
                self.stats[name]['week'] = extract_difference(chosen_from_ts, 'week', price)
                self.stats[name]['buy'] = price - buy

                amount = _sec_data['amount']
                self.stats[name]['buy_a'] = self.stats[name]['buy'] * amount

                self.stats[name]['year_r'] = calc_relative(price, self.stats[name]['year'])
                self.stats[name]['season_r'] = calc_relative(price, self.stats[name]['season'])
                self.stats[name]['month_r'] = calc_relative(price, self.stats[name]['month'])
                self.stats[name]['week_r'] = calc_relative(price, self.stats[name]['week'])
                self.stats[name]['buy_r'] = calc_relative(price, self.stats[name]['buy'])

        calc_difference()



