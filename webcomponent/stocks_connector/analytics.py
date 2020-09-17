from typing import List, Tuple, Union, Callable
import asyncio as asy
import dateutil.parser as parser
import datetime as dt
from .main import *
from storagecomponent.connector import *
from functools import partial
from settingscomponent.loader import loop
import pandas as pd
from webcomponent.stocks_connector.ana_dict import *


COMPACT_DELTA = dt.timedelta(days=99)
TIMEZONE = dt.timezone(dt.timedelta(hours=3))
LAG = dt.timedelta(minutes=15)
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
    async def get_new_data(symbols: List[str], date: Union[datetime, None] = None, force_load: bool = False) -> List[dict]:
        now = dt.datetime.now()  # tz=TIMEZONE)
        connector = VantageConnector(symbols)

        async def daily_full():
            _data, _symbols = await connector.daily_adjusted('full')
            return _data, _symbols

        if date is None or date < now - COMPACT_DELTA or force_load:
            data, _ = await daily_full()
        else:
            data, _ = await connector.daily_adjusted()

        return data


class DataAnalysis:
    @classmethod
    async def create(cls, secondary_data: List[dict], with_preload=True):
        self = cls(secondary_data)
        if with_preload:
            await self.preload()
        await self.preload_data_pipe()
        return self

    def __init__(self, secondary_data: List[dict]): #do not use this
        """class init
            args:
            secondary_data: List[dict]
                list of necessary symbols to calculate statistics with format from yaml config
        """
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
                             data_save_func: Callable):
        date = await date_func()
        actuality = False
        if date is not None: #this fragment tests if there is need for downloading full data (caching)
            date: datetime = date['date']
            today = datetime.now().date()
            date_date = date.date()
            if date_date == today:
                actuality = True
        if not actuality:
            new_data = await data_download_func(date)
            _ = await data_save_func(new_data[0]) #CAN FALL IF NOTHINGH DOWNLOADED
            return _
        return None

    async def preload_data_pipe(self):
        """
        func organizes pipeline for async historical data loading
        """
        async def preload_with_filter():
            tasks = []

            for symbol in self.symbols:
                date_func = partial(find_last_record, symbol)
                data_download_func = partial(DataGetter.get_new_data, [symbol], force_load=True)
                data_save_func = partial(insert_daily_vantage, symbol)
                pipeline = self.data_load_pipe(date_func, data_download_func,
                                               data_save_func)
                tasks.append(pipeline)

            results: List[List[dict]] = await asy.gather(*tasks)
            return results

        async def apply_filter(func: Callable, date_inclusion=False):
            tasks = []
            for paper in self.secondary:
                symbol = paper['name']
                if date_inclusion:
                    date_of_buy = parser.parse(paper['date_of_buy'])
                    tasks.append(func(symbol, date_of_buy))
                else:
                    tasks.append(func(symbol))

            results = await asy.gather(*tasks)
            return results

        self.preloaded = await preload_with_filter()
        self.data_weekly = await apply_filter(aggreagate_daily_week)
        self.data_daily = await apply_filter(aggregate_daily)
        self.data_dividends = await apply_filter(aggregate_dividends, date_inclusion=True)
        self.dw_df = {}
        self.dd_df = {}
        for index, sec_data in enumerate(self.secondary):
            name = sec_data['name']
            self.dw_df[name] = pd.DataFrame(self.data_weekly[index])
            self.dd_df[name] = pd.DataFrame(self.data_daily[index])

    async def calc_stats(self):
        def ret_dict(weekly_ts, daily_ts) -> dict:
            result = {}
            for key, stat in STATS['weekly'].items():
                result[key] = weekly_ts[stat - 1]

            for key, stat in STATS['daily'].items():
                result[key] = daily_ts[stat - 1]

            return result

        def extract_difference(chosen, string, price):
            return price - chosen[string]['close']

        def calc_relative(before, offset):
            return ((before+offset)/before - 1) * 100

        def calc_div_sum(div_data):
            dividends = 0
            for point in div_data:
                dividends += point['dividends']

            return dividends


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

                dividends = calc_div_sum(self.data_dividends[index])
                self.stats[name]['roi'] = price + dividends - buy
                self.stats[name]['roi_a'] = self.stats[name]['roi'] * amount
                self.stats[name]['roi_r'] = calc_relative(price+dividends, self.stats[name]['buy'])

                self.stats[name]['year_r'] = calc_relative(price, self.stats[name]['year'])
                self.stats[name]['season_r'] = calc_relative(price, self.stats[name]['season'])
                self.stats[name]['month_r'] = calc_relative(price, self.stats[name]['month'])
                self.stats[name]['week_r'] = calc_relative(price, self.stats[name]['week'])
                self.stats[name]['buy_r'] = calc_relative(price, self.stats[name]['buy'])

        calc_difference()


class DataAnalysisYF:

    @classmethod
    async def create(cls, secondary_data: List[dict]):
        self = cls(secondary_data)
        await self.preload()
        await self.filter_data()
        await self.calc_stats()
        return self

    def __init__(self, secondary_data: List[dict]):
        self.secondary_data = secondary_data
        self.symbols = [data['name'] for data in secondary_data]

    async def preload(self):
        connector = YFinanceConnector(self.symbols)
        history = connector.get_history()
        redone_history = []

        for points, symbol in zip(history, self.symbols):
            for key, value in points.items():
                converted = key.to_pydatetime()
                value['symbol'] = symbol
                value['date'] = converted
                redone_history.append(value)

        inserted = await save_yf_history(redone_history, self.symbols)
        self.history = history


    async def filter_data(self):
        async def _to_df(func):
            data = await func
            return pd.DataFrame(data)

        week_tasks = [_to_df(aggregate_yf_week(symbol)) for symbol in self.symbols]
        day_tasks = [_to_df(aggregate_yf_day(symbol)) for symbol in self.symbols]
        month_tasks = [_to_df(aggregate_yf_month(symbol)) for symbol in self.symbols]
        self.weeks = await asy.gather(*week_tasks)
        self.days = await asy.gather(*day_tasks)
        self.months = await asy.gather(*month_tasks)

    async def calc_stats(self):
        self.stats = {}

        def calc_relative(buy, current):
            return (current - buy)*100/buy

        for index, info in enumerate(self.secondary_data):
            name = info['name']

            daily = self.days[index]
            weekly = self.weeks[index]
            monthly = self.months[index]
            buy_price = info['buy']
            price = daily.iloc[0].Close
            amount = info['amount']
            if name not in self.stats:
                self.stats[name] = {}
            self.stats[name]['buy_relative'] = calc_relative(buy_price, price)
            self.stats[name]['buy_difference'] = price - buy_price
            self.stats[name]['buy_absolute'] = (price - buy_price)*amount
            if 'date_of_buy' in info:
                date_of_buy = parser.parse(info['date_of_buy'])
                dividends = await aggregate_dividends_yf(name, date_of_buy)
            else:
                dividends = 0
            self.stats[name]['roi_relative'] = calc_relative(buy_price, price+dividends)
            self.stats[name]['roi_difference'] = price + dividends - buy_price
            self.stats[name]['roi_absolute'] = (price + dividends - buy_price)*amount
            self.stats[name]['div'] = dividends*amount
            self.stats[name]['absolute'] = str(round(price*amount, 2))+'/'

        buffer_array = []
        for symbol, stats in self.stats.items():
            mod_stats = {}

            for key in rows_list:
                mod_stats[translation_dict[key]] = stats[key]

            buffer_array.append(mod_stats)

        self.stats_df = pd.DataFrame(buffer_array, index=self.symbols)












