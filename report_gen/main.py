import weasyprint as wp
from jinja2 import Environment, FileSystemLoader
import pandas as pd
import os
path = os.path.abspath('../')
os.chdir(path)
from webcomponent.stocks_connector.analytics import DataAnalysisYF, SharesMoexAnalysis, BondsMoexAnalysis
from webcomponent.stocks_connector.helpers import map_dict

import asyncio
from settingscomponent.loader import SEQURITIES
import numpy as np
from bs4 import BeautifulSoup as bs
seq_lots_YF = ['international']
seq_lots_M = ['shares']
seq_lots_B = ['bonds']

basis = ['BOMNDS_MOEX', 'MOEX', 'INTERNATIONAL']

env = Environment(loader=FileSystemLoader('./report_gen'))
template = env.get_template('template.html')
loop = asyncio.get_event_loop()


def get_sequrities_YF(seq_lots):

    tasks = [DataAnalysisYF.create(SEQURITIES[lot]) for lot in seq_lots]
    return tasks

def get_sequrities_M(seq_lots):

    tasks = [SharesMoexAnalysis.create(SEQURITIES[lot]) for lot in seq_lots]
    return tasks

def get_sequrities_B(seq_lots):

    tasks = [BondsMoexAnalysis.create(SEQURITIES[lot]) for lot in seq_lots]
    return tasks

async def fetch_tasks(tasks):

    data = await asyncio.gather(*tasks)
    return data

to_do = fetch_tasks(get_sequrities_B(seq_lots_B)+get_sequrities_M(seq_lots_M)+get_sequrities_YF(seq_lots_YF))

data_seq = loop.run_until_complete(to_do)


formatted_data = []
for seq, data in zip(basis, data_seq):
    html = data.stats_df.to_html(float_format=lambda x: '%.2f' % x)
    souped_table: bs = bs(html)
    cols = data.stat_cols
    max_col = len(cols) - 1
    tds = souped_table.find_all('td')

    current_col = 0
    for td in tds:
        if current_col > max_col:
            current_col = 0
        col_name = cols[current_col]

        current_col += 1

        map_task = map_dict[col_name]
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
            if map_task == 'per' or map_task == 'abs':
                if number > 0:
                    td['class'] = prev_class + 'plus'
                elif number < 0:
                    td['class'] = prev_class + 'minus'
            elif map_task == 'per_b':
                if number > 100:
                    td['class'] = prev_class + 'plus'
                elif number < 100:
                    td['class'] = prev_class + 'minus'


    formatted_data.append((seq, souped_table.prettify()))

template_var = {
    'title': 'TESSSST',
    'roi_tables': formatted_data,
}
html_out = template.render(template_var)
wp.HTML(string=html_out).write_pdf("report.pdf", stylesheets=['report_gen/typography.css'])
