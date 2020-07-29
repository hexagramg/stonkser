from typing import List, Tuple, Union
import dateutil.parser as parser
import datetime as dt
from .main import *

COMPACT_DELTA = dt.timedelta(days=99)
TIMEZONE = dt.timezone(dt.timedelta(hours=3))


class DataGetter:

    @staticmethod
    async def get_new_data(symbols: List[str], date: Union[str, None] = None):
        now = dt.datetime.now()#tz=TIMEZONE)
        connector = VantageConnector(symbols)

        async def daily_full():
            _data, _symbols = await connector.daily_adjusted('full')
            return _data, _symbols

        try:
            parsed_date = parser.parse(date)
        except (ValueError, TypeError) as e:
            data, _ = await daily_full()
        else:
            if parsed_date < now - COMPACT_DELTA:
                data, _ = await daily_full()
            else:
                data, _ = await connector.daily_adjusted()

        return data, symbols

