from settingscomponent.loader import SETTINGS
import motor.motor_asyncio as mot_asy
from typing import List, Union, Tuple
import dateutil.parser as parser
from datetime import datetime
from pymongo import ReturnDocument
DB_key = SETTINGS['DB']
CLIENT = mot_asy.AsyncIOMotorClient(DB_key)
DB = CLIENT['rewampStock']


async def find_sequrity(identity: str, upsert=True):
    document = await DB['sequrities'].find_one({'symbol': identity})
    return document


def extract_symbol(func):
    async def finder(symbol, *args, **kwargs):
        sequrity = await find_sequrity(symbol)

        if sequrity is None:
            return None
        result = await func(symbol, *args, sequrity=sequrity, **kwargs)
        return result

    return finder


async def insert_sequrity(sequrity: dict):
    mapped_dict = {}
    for key, value in sequrity.items():
        if '. ' in key:
            _key = ' '.join(key.split(' ')[1:])
        else:
            _key = key

        mapped_dict[_key] = value
        document = await DB['sequrities'].find_one_and_replace(
            {
                'symbol': mapped_dict['symbol']
            },
            mapped_dict,
            upsert=True,
            return_document=ReturnDocument.AFTER
        )
        #document = await DB['sequrities'].insert_one(mapped_dict)
    return document

@extract_symbol
async def insert_daily_vantage(symbol: str, vantage_daily: dict, sequrity=None):
    _projection = ['date']
    new_time = set()
    pipeline = [
        {'$match': {'seq_id': sequrity['_id'], 'type': 'daily_adjusted'}},
        {'$group': {'_id': None, 'dates': {'$addToSet': '$date'}}}
    ]
    agg_cursor = DB['time_series'].aggregate(pipeline)
    async for aggregation in agg_cursor:
        new_time = set(aggregation['dates'])

    formatted_array = []
    for key, value in vantage_daily.items():
        parsed_date = parser.parse(timestr=key)
        if parsed_date not in new_time:
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
    if formatted_array:
        results = await DB['time_series'].insert_many(formatted_array)
        return results

    return None

@extract_symbol
async def aggreagate_daily_month(symbol: str, sequrity=None):

    pipeline = [
        {'$match': {
            'seq_id': sequrity['_id'],
            'type': 'daily_adjusted'
        }},
        {
           '$group': {
               '_id': {
                   '$dateFromParts': {
                        'isoWeekYear': {
                            '$isoWeekYear': '$date'
                        },
                        'isoWeek': {
                            '$isoWeek': '$date'
                        },
                   }
               },
               'close': {
                   '$avg': '$adjusted close'
               },
               'volume_avg': {
                   '$avg': '$volume'
               },
               'volume': {
                   '$sum': '$volume'
               },
           }
        },
        {
            '$sort': {
                '_id': -1
            }
        }
    ]
    cursor = DB['time_series'].aggregate(pipeline)
    result_list = await cursor.to_list(length=120)
    return result_list


@extract_symbol
async def aggregate_daily(symbol: str, length: int = 120, sequrity=None):

    pipeline = [
        {
            '$match': {
                'seq_id': sequrity['_id'],
                'type': 'daily_adjusted'
            }},
        {
            '$sort': {
                'date': -1
            }
        },
        {
            '$limit': length
        }
    ]

    cursor = DB['time_series'].aggregate(pipeline)
    result_list = await cursor.to_list(length=length)
    return result_list


@extract_symbol
async def find_last_record(symbol, sequrity=None) -> Union[dict, None]:
    pipeline = [
        {
            '$match': {
                'seq_id': sequrity['_id']
            }
        },
        {
            '$sort': {
                'date': -1
            }
        },
        {
            '$limit': 1
        }
    ]

    cursor = DB['time_series'].aggregate(pipeline)
    result_list = await cursor.to_list(length=1)
    if result_list:
        return result_list[0]
    else:
        return None







