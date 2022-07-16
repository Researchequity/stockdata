from datetime import datetime
import datetime as dt
import pandas as pd
import numpy as np
import os
import getpass
from filepath import *

# Input from Excel
# if getpass.getuser() == 'ankit':
#     data_excel_file = python_ankit_dir + "\\Reporting_ankit.xlsm"
# if getpass.getuser() == 'VIJITR':
#     data_excel_file = python_ankit_dir + "\\Reporting_.xlsm"
#
# wb = xw.Book(data_excel_file)

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

division_factor = 375
# tradewatch = "Y:\\aggregate\\tradewatch"

# print(datetime.today() - dt.timedelta(minutes=30))
DATE_TODAY = '20210507'
DATE_TODAY = ''.join(str(datetime.today().date()).split('-'))

df_norman_trd = pd.read_csv(python_ankit_dir + '/Average.csv')
df_norman_trd['normean_quantity'] = df_norman_trd['vol_norm_mean'] // division_factor  # mean_quantity
df_norman_trd['normean_trd'] = df_norman_trd['trd_norm_mean'] // division_factor
df_norman_trd = df_norman_trd[['Stock', 'normean_quantity', 'normean_trd']]
df_norman_trd.rename(columns={'Stock': 'Symbol'}, inplace=True)

resulting_filepath = aggregate_file_dir + '/tradeWatch_historical_{}.csv'.format(DATE_TODAY)
df_5_groupby = pd.read_csv(resulting_filepath, header=None)  # keep same
df_5_groupby.columns = ['Date', 'token', 'Symbol', 'row_count', 'sum_qty', 'nbuy_count', 'nsell_count', 'vwap_price',
                        'nbuy_qty', 'nsell_qty', 'cbuy_count', 'cbuy_qty', 'cbuy_sum', 'csell_count',
                            'csell_qty', 'csell_sum']
df_5_groupby['Date'] = pd.to_datetime(df_5_groupby['Date'])

last_min = pd.to_datetime(df_5_groupby['Date'].max())
TODAY_DAY = last_min.day
TODAY_MONTH = last_min.month
TODAY_YEAR = last_min.year

BTST_date = datetime(day=TODAY_DAY, month=TODAY_MONTH, year=TODAY_YEAR, hour=14, minute=30, second=0)

df_5_groupby = df_5_groupby[df_5_groupby['Date'] >= BTST_date]

# df_5_groupby = df_5_groupby[df_5_groupby['token'] == 15332]

OPENING_DATA_FILE = aggregate_file_dir + '/tradewatch/openingdata_{}.csv'.format(DATE_TODAY)
open_data = pd.read_csv(OPENING_DATA_FILE, header=None)
open_data.rename(columns={0: 'token', 1: 'o_price'}, inplace=True)

df_6 = pd.merge(df_5_groupby, open_data, on=['token'])
df_6 = pd.merge(df_6, df_norman_trd, on='Symbol')
df_6 = df_6[["Date", "token", "normean_quantity", "normean_trd", "nbuy_count", "nsell_count", "nbuy_qty", "nsell_qty",
             "row_count", "sum_qty", "o_price", "vwap_price", "Symbol"]]

stockmetadata_df = pd.read_csv(python_ankit_dir + '/StockMetadata.csv')
stockmetadata_df = stockmetadata_df[['Symbol', 'MarketCap']]
df_6 = df_6.merge(stockmetadata_df, on='Symbol', how='left')

df_6['r_qty'] = df_6['sum_qty'] // df_6['normean_quantity']
df_6['r_trade'] = df_6['row_count'] // df_6['normean_trd']
df_6['BUY'] = df_6['nbuy_qty'] // df_6['nsell_qty']
df_6.dropna(subset=['Symbol'], inplace=True)

# 3 conditions row_count and o_price and qty > x * mean
cond_b = ((df_6['row_count'] >= 100) & (df_6['vwap_price'] > df_6['o_price'])
          & ((df_6['sum_qty'] > 3 * df_6['normean_quantity']) & (df_6['MarketCap'] == 'LARGE')
             | (df_6['sum_qty'] > 4 * df_6['normean_quantity']) & (df_6['MarketCap'] == 'Mid')
             | (df_6['sum_qty'] > 10 * df_6['normean_quantity']) & (df_6['MarketCap'] == 'Small')
             | (df_6['sum_qty'] > 20 * df_6['normean_quantity']) & (df_6['MarketCap'] == 'VSM')))

df_6['repeat_b'] = np.where(cond_b, 1, 0)
df_6['repeat_bc'] = df_6.groupby(["token"]).agg({'repeat_b': ['cumsum']})

cond_s = ((df_6['row_count'] >= 100) & (df_6['vwap_price'] < df_6['o_price'])
          & ((df_6['sum_qty'] > 3 * df_6['normean_quantity']) & (df_6['MarketCap'] == 'LARGE')
             | (df_6['sum_qty'] > 4 * df_6['normean_quantity']) & (df_6['MarketCap'] == 'Mid')
             | (df_6['sum_qty'] > 10 * df_6['normean_quantity']) & (df_6['MarketCap'] == 'Small')
             | (df_6['sum_qty'] > 20 * df_6['normean_quantity']) & (df_6['MarketCap'] == 'VSM')))

df_6['repeat_s'] = np.where(cond_s, 1, 0)
df_6['repeat_sc'] = df_6.groupby(["token"]).agg({'repeat_s': ['cumsum']})
df_6['repeat_sc'] = df_6.groupby(["token"]).agg({'repeat_s': ['cumsum']})

df_6 = df_6[df_6['Date'] == df_6['Date'].max()][["Date", "token", "Symbol", "MarketCap", "repeat_bc", "repeat_sc"]]
# wb.sheets("BTST_buy").range("A1").options(index=False).value = df_6.sort_values("repeat_bc", ascending=False)
df_6_csv = df_6.sort_values("repeat_bc", ascending=False)
df_6_csv.to_csv(aggregate_file_dir + '//report//watch_BTST_perm_BTST_buy.csv', index=None)
# df_6 = df_6[df_6['Date'] == df_6['Date'].max()][["Date", "token", "Symbol","MarketCap","repeat_sc"]]
# wb.sheets("sell_new").range("A1").options(index = False).value = df_6.sort_values("repeat_sc", ascending=False)





