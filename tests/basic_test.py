from storagecomponent.connector import *
import asyncio
from webcomponent.stocks_connector.analytics import  DataAnalysisYF, SharesMoexAnalysis, BondsMoexAnalysis
from webcomponent.stocks_connector.main import VantageConnector
from settingscomponent.loader import SEQURITIES
import seaborn as sns
import matplotlib.pyplot as plt
loop = asyncio.get_event_loop()
data = loop.run_until_complete(DataAnalysisYF.create(SEQURITIES['international']))
data_moex = loop.run_until_complete(SharesMoexAnalysis.create(SEQURITIES['shares']))
data_bonds = loop.run_until_complete(BondsMoexAnalysis.create(SEQURITIES['bonds']))

