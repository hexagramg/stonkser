from .ana_dict import *
import pandas as pd
import dateutil.parser as parser
from datetime import datetime

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


def base_dict_to_df(stats: dict, symbols: list, _rows_list=cols_list_shares):
    buffer_array = []
    for symbol, stats in stats.items():
        mod_stats = {}

        for key in _rows_list:
            if key in translation_dict:
                mod_stats[translation_dict[key]] = stats[key]
            else:
                mod_stats[key] = stats[key]
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
#   for colname in exclude_coloring:
#    #POTENTIALLY HAZARDOUS
#       #TODO MAKE FAILSAFE
#       translated = translation_dict[colname]
#       stats_df[translated] = stats_df[translated].map(lambda x: str(round(x, 2)) + '/')
    return stats_df


def calc_weighed_hist(hist_df, close_col, tradedate_col, name, amount, global_weighted=None):
    """
    Args:
        hist_df: dataframe of one symbols historical prices, has columns TRADEDATE and CLOSE
        close_col: name of the close data column
        tradedate_col: name of the date of trade column
        name: symbols name
        amount: amount of symbols stocks
        global_weighted: global dataframe that is a bybproduct of joins of weighted_hist
    """
    # W_CLOSE stands for weighted close and represents current absolute value at that point in time
    hist_df['W_CLOSE'] = hist_df[close_col].map(lambda x: x * amount)
    try:
        hist_df[tradedate_col] = hist_df[tradedate_col].map(lambda x: parser.parse(x))
    except TypeError:
        hist_df[tradedate_col] = hist_df[tradedate_col].map(lambda x: x.to_pydatetime())
    #if not hist_df[tradedate_col]._is_datelike_mixed_type:
    #   hist_df[tradedate_col] = hist_df[tradedate_col].map(lambda x: parser.parse(x))

    weighted_abs = {name: hist_df['W_CLOSE'].to_list()}
    weighted_df = pd.DataFrame(weighted_abs, index=hist_df[tradedate_col].to_list())
    if global_weighted is None:
        global_weighted = weighted_df
    else:
        global_weighted = global_weighted.join(weighted_df)

    return global_weighted

def add_weighted_stats(weight_df):
    weight_df['absolute'] = weight_df.sum(axis=1)
    weight_df['week_date'] = weight_df.index.map(lambda x: datetime.fromisocalendar(x.year, x.week, 1))

    return weight_df
