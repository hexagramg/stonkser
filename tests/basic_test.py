from storagecomponent.connector import insert_sequrity, find_sequrity,insert_daily_vantage
import asyncio
from webcomponent.stocks_connector.analytics import DataGetter
from webcomponent.stocks_connector.main import VantageConnector

loop = asyncio.get_event_loop()
vc = VantageConnector(['GOOG', 'AAPL'])
task_data = DataGetter.get_new_data(['GOOG', 'AAPL'])
task_symbol = vc.get_quotes()
task_find = find_sequrity('GOOG')
