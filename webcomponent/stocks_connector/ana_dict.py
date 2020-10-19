translation_dict = {
    'buy_relative': 'Результат, %',
    'buy_absolute': 'Абсолютный рез., ye.',
    'roi_relative': 'ROI, %',
    'roi_absolute': 'ROI, ye.',
    'absolute': 'Сумма влож, ye.',
    'div': 'Дивиденды, ye.',
    'coupon': 'Купон, ye.',
    'coupon_date': 'Дата купона',
    'current_yield': 'Тек доходность, %',
    'buy_yield': 'Buy доходность, %',
    'buy': 'Покупка, %',
    'current': 'Текущая, %'
}

cols_list_shares = ('buy_relative', 'roi_relative', 'div', 'buy_absolute', 'roi_absolute', 'absolute')

cols_list_bonds = ('buy', 'current', 'buy_yield', 'current_yield', 'coupon_date', 'coupon', 'absolute')

#exclude_coloring = ['absolute']

map_dict = {
    'buy_relative': 'per',
    'buy_absolute': 'abs',
    'roi_relative': 'per',
    'roi_absolute': 'abs',
    'absolute': '-',
    'div': 'abs',
    'coupon': '-',
    'coupon_date': '-',
    'current_yield': '-',
    'buy_yield': '-',
    'buy': 'per_b',
    'current': 'per_b'
}
