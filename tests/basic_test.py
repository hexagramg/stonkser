from storagecomponent.connector import *
import asyncio
from webcomponent.stocks_connector.analytics import DataGetter, DataAnalysis
from webcomponent.stocks_connector.main import VantageConnector
from settingscomponent.loader import SEQURITIES
loop = asyncio.get_event_loop()
data = loop.run_until_complete(DataAnalysis.create(SEQURITIES['international']))
loop.run_until_complete(data.calc_stats())

