from .ana_dict import *
import pandas as pd

def calc_base(buy_price, price, amount, dividends):
    """
    method for base calculation unification

    Returns:
        dict of calculated metrics
    """
    def calc_relative(buy, current):
        return (current - buy)*100/buy
    base = {'buy_relative': calc_relative(buy_price, price),
            'buy_difference': price - buy_price,
            'buy_absolute': (price - buy_price) * amount,
            'roi_relative': calc_relative(buy_price, price + dividends),
            'roi_difference': price + dividends - buy_price,
            'roi_absolute': (price + dividends - buy_price) * amount,
            'div': dividends * amount,
            'absolute': price * amount}
    return base


def base_dict_to_df(stats: dict, symbols: list, _rows_list=rows_list):
    buffer_array = []
    for symbol, stats in stats.items():
        mod_stats = {}

        for key in _rows_list:
            mod_stats[translation_dict[key]] = stats[key]

        buffer_array.append(mod_stats)

    stats_df = pd.DataFrame(buffer_array, index=symbols)
    stats_df.sort_values(by=[translation_dict['absolute']], inplace=True)
    return stats_df


def calc_total(stats_df):
    total_row = []
    translated_abs = translation_dict['absolute']
    abs = stats_df[translated_abs]
    abs_sum = abs.sum()
    for column_name in stats_df:
        column = stats_df[column_name]
        if '%' in column_name:
            top = column * abs
            total_row.append(top.sum() / abs_sum)
        elif 'ye.' in column_name:
            total_row.append(column.sum())
        else:
            total_row.append(0)
    total_df = pd.DataFrame([total_row], index=['Итог'], columns=stats_df.columns)
    stats_df = stats_df.append(total_df)
    for colname in exclude_coloring:
        #POTENTIALLY HAZARDOUS
        #TODO MAKE FAILSAFE
        translated = translation_dict[colname]
        stats_df[translated] = stats_df[translated].map(lambda x: str(round(x, 2)) + '/')
    return stats_df