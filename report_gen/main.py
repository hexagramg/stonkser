import weasyprint as wp
from jinja2 import Environment, FileSystemLoader
import pandas as pd
import os
path = os.path.abspath('../')
os.chdir(path)
from webcomponent.stocks_connector.analytics import DataAnalysisYF, SharesMoexAnalysis
import asyncio
from settingscomponent.loader import SEQURITIES
import numpy as np
from bs4 import BeautifulSoup as bs
seq_lots_YF = ['international']
seq_lots_M = ['shares']

basis = ['MOEX', 'INTERNATIONAL']

env = Environment(loader=FileSystemLoader('./report_gen'))
template = env.get_template('template.html')
loop = asyncio.get_event_loop()


def get_sequrities_YF():

    tasks = [DataAnalysisYF.create(SEQURITIES[lot]) for lot in seq_lots_YF]
    return tasks

def get_sequrities_M():

    tasks = [SharesMoexAnalysis.create(SEQURITIES[lot]) for lot in seq_lots_M]
    return tasks

async def fetch_tasks(tasks):

    data = await asyncio.gather(*tasks)
    return data

to_do = fetch_tasks(get_sequrities_M()+get_sequrities_YF())

data_seq = loop.run_until_complete(to_do)


formatted_data = []
for seq, data in zip(basis, data_seq):
    html = data.stats_df.to_html(float_format=lambda x: '%.2f' % x)
    souped_table: bs = bs(html)

    tds = souped_table.find_all('td')
    for td in tds:
        value = td.string
        try:
            number = float(value)
        except (ValueError, TypeError) as e:
            if len(value) > 1:
                if value[-1] == '/':
                    td.string = value[:-1]
        else:
            if 'class' in td:
                prev_class = td['class'] + ' '
            else:
                prev_class = ''
            if number > 0:
                td['class'] = prev_class + 'plus'
            elif number < 0:
                td['class'] = prev_class + 'minus'

    formatted_data.append((seq, souped_table.prettify()))

template_var = {
    'title': 'TESSSST',
    'roi_tables': formatted_data,
}
html_out = template.render(template_var)
wp.HTML(string=html_out).write_pdf("report.pdf", stylesheets=['report_gen/typography.css'])
