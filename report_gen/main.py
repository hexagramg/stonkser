import weasyprint as wp
from jinja2 import Environment, FileSystemLoader
import pandas as pd
import os
path = os.path.abspath('../')
os.chdir(path)
from webcomponent.stocks_connector.analytics import DataAnalysisYF
import asyncio
from settingscomponent.loader import SEQURITIES
import numpy as np
from bs4 import BeautifulSoup as bs
seq_lots = ['moex', 'inter']


env = Environment(loader=FileSystemLoader('./report_gen'))
template = env.get_template('template.html')
loop = asyncio.get_event_loop()


async def get_sequrities():
    tasks = [DataAnalysisYF.create(SEQURITIES[lot]) for lot in seq_lots]
    data = await asyncio.gather(*tasks)
    return data

data_seq = loop.run_until_complete(get_sequrities())

formatted_data = []
for seq, data in zip(seq_lots, data_seq):
    html = data.stats_df.to_html(float_format=lambda x: '%.2f' % x)
    souped_table: bs = bs(html)

    tds = souped_table.find_all('td')
    for td in tds:
        value = td.string
        try:
            number = float(value)
        except ValueError as e:
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
    'seq': seq_lots
}
html_out = template.render(template_var)
wp.HTML(string=html_out).write_pdf("report.pdf", stylesheets=['report_gen/typography.css'])
