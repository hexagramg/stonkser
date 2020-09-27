from storagecomponent.connector import *
import asyncio
from webcomponent.stocks_connector.analytics import DataGetter, DataAnalysis, DataAnalysisYF, SharesMoexAnalysis
from webcomponent.stocks_connector.main import VantageConnector
from settingscomponent.loader import SEQURITIES
loop = asyncio.get_event_loop()
data = loop.run_until_complete(DataAnalysisYF.create(SEQURITIES['international']))
data_moex = loop.run_until_complete(SharesMoexAnalysis.create(SEQURITIES['shares']))

