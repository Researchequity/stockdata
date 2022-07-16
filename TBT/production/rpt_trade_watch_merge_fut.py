# Create Pre open mean file of all previous day file
import pandas as pd
from glob import glob
from filepath import *
import getpass
from datetime import datetime
import datetime as dt
import numpy as np
import os
from dateutil.relativedelta import relativedelta

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

# Input from Excel
# if getpass.getuser() == 'ankit':
#     data_excel_file = python_ankit_drive_f + "\\Reporting_ankit.xlsm"
# if getpass.getuser() == 'VIJITR':
#     data_excel_file = python_ankit_drive_f + "\\Reporting_.xlsm"
# if getpass.getuser() == 'rohank':
#     data_excel_file = python_ankit_drive_f + "\\Reporting_rohan.xlsm"
#
# wb = xw.Book(data_excel_file)

# DATE_TODAY = '20210806'
DATE_TODAY = ''.join(str(datetime.today().date()).split('-'))
filepath = aggregate_file_dir + "tradewatch_fut//tradeWatch_historical_" + DATE_TODAY + ".csv"


def watch_perm_trades_stream_op():

    division_factor = 375

    df_norman_trd = pd.read_csv(python_ankit_dir + '/Average_fut.csv')
    df_norman_trd['normean_quantity'] = df_norman_trd['vol_norm_mean'] // division_factor  # mean_quantity
    df_norman_trd['normean_trd'] = 37500 // division_factor
    df_norman_trd = df_norman_trd[['Stock', 'normean_quantity', 'normean_trd', 'token', 'companyName']]
    df_norman_trd.rename(columns={'Stock': 'Symbol'}, inplace=True)

    df_5_groupby = pd.read_csv(filepath, header=None)  # keep same
    df_5_groupby.columns = ['Date', 'token', 'Symbol', 'row_count', 'sum_qty', 'nbuy_count', 'nsell_count',
                            'vwap_min',
                            'nbuy_qty', 'nsell_qty']
    df_5_groupby['Date'] = pd.to_datetime(df_5_groupby['Date'])


    OPENING_DATA_FILE = aggregate_file_dir + '/tradewatch_fut/openingdata_{}.csv'.format(DATE_TODAY)
    open_data = pd.read_csv(OPENING_DATA_FILE, header=None)
    open_data.rename(columns={0: 'token', 1: 'o_price'}, inplace=True)

    df_6 = pd.merge(df_5_groupby, open_data, on=['token'])
    df_6 = pd.merge(df_6, df_norman_trd, on=['Symbol', 'token'])
    df_6 = df_6[
        ["Date", "token", "normean_quantity", "normean_trd", "nbuy_count", "nsell_count", "nbuy_qty", "nsell_qty",
         "row_count", "sum_qty", "o_price", "vwap_min", "Symbol", "companyName"]]

    stockmetadata_df = pd.read_csv(python_ankit_dir + '/StockMetadata.csv')
    stockmetadata_df = stockmetadata_df[['Symbol', 'MarketCap']]
    df_6 = df_6.merge(stockmetadata_df, on='Symbol', how='left')

    df_6['r_qty'] = df_6['sum_qty'] // df_6['normean_quantity']
    df_6['r_trade'] = df_6['row_count'] // df_6['normean_trd']
    df_6['BUY'] = df_6['nbuy_qty'] // df_6['nsell_qty']
    df_6.dropna(subset=['Symbol'], inplace=True)



    # calc vwap
    ohlc_filepath = aggregate_file_dir + '/tradewatch_fut/ohlc_{}.csv'.format(DATE_TODAY)
    df_ohlc = pd.read_csv(ohlc_filepath, header=None)[[0, 1, 6, 7]]
    df_ohlc[0] = pd.to_datetime(df_ohlc[0])
    df_ohlc[["vwap_all"]] = df_ohlc.groupby([1])[6].cumsum(axis=0) // df_ohlc.groupby([1])[7].cumsum(axis=0)

    df_ohlc = df_ohlc[[0, 1, "vwap_all"]]
    df_ohlc.columns = ['Date', 'token', 'vwap_all']
    df_6 = pd.merge(df_6, df_ohlc, on=['Date', 'token'])
    df_6_buy = df_6[df_6['vwap_min'] > df_6['vwap_all']]
    df_6_sell = df_6[df_6['vwap_min'] < df_6['vwap_all']]

    # 3 conditions row_count and o_price and qty > x * mean
    cond_b = ((df_6_buy['row_count'] >= 20) & (df_6_buy['vwap_min'] > df_6_buy['o_price'])
              & ((df_6_buy['sum_qty'] > 3 * df_6_buy['normean_quantity']) & (df_6_buy['MarketCap'] == 'LARGE')
                 | (df_6_buy['sum_qty'] > 3 * df_6_buy['normean_quantity']) & (df_6_buy['MarketCap'] == 'Mid')
                 | (df_6_buy['sum_qty'] > 3 * df_6_buy['normean_quantity']) & (df_6_buy['MarketCap'] == 'Small')
                 | (df_6_buy['sum_qty'] > 15 * df_6_buy['normean_quantity']) & (df_6_buy['MarketCap'] == 'VSM')))

    df_6_buy['repeat_b'] = np.where(cond_b, 1, 0)
    df_6_buy['repeat_bc'] = df_6_buy.groupby(["token"]).agg({'repeat_b': ['cumsum']})

    df_6_buy = df_6_buy.groupby(["token", "Symbol", "MarketCap"])
    df_6_buy = df_6_buy.tail(1)[
        ["Date", "token", "Symbol", "MarketCap", "repeat_bc", "vwap_min", "vwap_all", "companyName"]]

    cond_s = ((df_6_sell['row_count'] >= 20) & (df_6_sell['vwap_min'] < df_6_sell['o_price'])
              & ((df_6_sell['sum_qty'] > 3 * df_6_sell['normean_quantity']) & (df_6_sell['MarketCap'] == 'LARGE')
                 | (df_6_sell['sum_qty'] > 3 * df_6_sell['normean_quantity']) & (df_6_sell['MarketCap'] == 'Mid')
                 | (df_6_sell['sum_qty'] > 3 * df_6_sell['normean_quantity']) & (df_6_sell['MarketCap'] == 'Small')
                 | (df_6_sell['sum_qty'] > 15 * df_6_sell['normean_quantity']) & (df_6_sell['MarketCap'] == 'VSM')))

    df_6_sell['repeat_s'] = np.where(cond_s, 1, 0)
    df_6_sell['repeat_sc'] = df_6_sell.groupby(["token"]).agg({'repeat_s': ['cumsum']})
    df_6_sell = df_6_sell.groupby(["token", "Symbol", "MarketCap"])
    df_6_sell = df_6_sell.tail(1)[
        ["Date", "token", "Symbol", "MarketCap", "repeat_sc", "vwap_min", "vwap_all", "companyName"]]

    buy_new = df_6_buy.sort_values("repeat_bc", ascending=False)
    sell_new = df_6_sell.sort_values("repeat_sc", ascending=False)

    return buy_new, sell_new


def call_fut():
    buy_new, sell_new = watch_perm_trades_stream_op()
    # wb.sheets("fut_buy").range("A1").options(index=False).value = buy_new
    # wb.sheets("fut_sell").range("A1").options(index=False).value = sell_new
    buy_new.to_csv(aggregate_file_dir + '//report//trade_watch_merge_fut_buy.csv', index=None)
    sell_new.to_csv(aggregate_file_dir + '//report//trade_watch_merge_fut_sell.csv', index=None)

    # pivot_temp_buy_df, pivot_temp_sell_df = SL_Daily_test()
    # max_prog = tradeWatch_historical()

    # merging dataframes buy
    # df_merge_buy = pd.merge(buy_new, max_prog, on=['Symbol'], how='left')
    # # df_merge_buy = pd.merge(df_merge_buy, pivot_temp_buy_df, on=['Symbol', 'MarketCap', 'token'], how='left')
    # print(df_merge_buy.head(20))
    # wb.sheets("fut_buy").range("A1").options(index=False).value = df_merge_buy
    #
    # # merging dataframes Sell
    # df_merge_sell = pd.merge(sell_new, pivot_temp_sell_df, how='left')
    # print(df_merge_sell.head(20))
    # wb.sheets("fut_sell").range("A1").options(index=False).value = df_merge_sell


if __name__ == '__main__':
    call_fut()



