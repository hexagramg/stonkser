from settingscomponent.loader import SETTINGS
import motor.motor_asyncio as mot_asy
from typing import List, Union, Tuple
import dateutil.parser as parser
from datetime import datetime
DB_key = SETTINGS['DB']
CLIENT = mot_asy.AsyncIOMotorClient(DB_key)
DB = CLIENT['rewampStock']


async def find_sequrity(identity: str, upsert=True):
    document = await DB['sequrities'].find_one({'symbol': identity}, upsert=upsert)
    return document


async def insert_sequrity(sequrity: dict):
    document = await DB['sequrities'].insert_one(sequrity)
    return document


async def insert_daily_vantage(vantage_daily: dict, symbol: str):
    sequrity = await find_sequrity(symbol)
    all_time = {}
    _projection = ['date']
    date_cursor = DB['time_series'].find({
        'seq_id': sequrity['_id'],
        'type': 'daily_adjusted'
    }, projection=_projection)
    for date_doc in await date_cursor.to_list(length=100):
        all_time |= {date_doc['date']}

    formatted_array = []
    for key, value in vantage_daily.items():
        parsed_date = parser.parse(timestr=key)
        if parsed_date not in all_time:
            new_dict = {
                'date': parsed_date,
                'seq_id': sequrity['_id'],
                'type': 'daily_adjusted'
            }
            for _key, _value in value.items():
                try:
                    new_dict[' '.join(_key.split(' ')[1:])] = float(_value)
                except KeyError as e:
                    pass
            formatted_array.append(new_dict)

    results = await DB['time_series'].insert_many(formatted_array)
    return results






