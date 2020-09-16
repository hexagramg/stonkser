import weasyprint as wp
from jinja2 import Environment, FileSystemLoader
import pandas as pd
from webcomponent.stocks_connector.analytics import DataAnalysisYF
import asyncio
from settingscomponent.loader import SEQURITIES
import numpy as np

html = '''<!DOCTYPE html>
<html>
<head lang="en">
    <meta charset="UTF-8">
    <title>{{ title }}</title>
</head>
<body>
    <h2>probably a test of a table</h2>
     {{ test_table}}
</body>
</html>'''

env = Environment(loader=FileSystemLoader('./report_gen'))
template = env.get_template('template.html')
loop = asyncio.get_event_loop()
data = loop.run_until_complete(DataAnalysisYF.create(SEQURITIES['moex']))


template_var = {
    'title': 'TESSSST',
    'test_table': data.stats_df.to_html(float_format=lambda x: '%.2f' % x)
}
html_out = template.render(template_var)
wp.HTML(string=html_out).write_pdf("report.pdf", stylesheets=['report_gen/typography.css'])
