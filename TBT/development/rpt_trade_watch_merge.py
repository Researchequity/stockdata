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
from get_stream_data_by_token_dev import *

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
filepath = aggregate_file_dir + "/tradeWatch_historical_" + DATE_TODAY + ".csv"


def SL_Daily_test():
    SL_all_df = pd.DataFrame()

    SL_mean = pd.read_csv(aggregate_file_dir + "sl//slaverage.csv")

    for file in glob(aggregate_file_dir + 'sl/scan//sl_{}_*.csv'.format(DATE_TODAY)):
        print(file)
        SL_df = pd.read_csv(file, header=None)
        SL_all_df = pd.concat([SL_all_df, SL_df])

    result = SL_all_df.groupby([3, 4]).agg({0: ['count'], 6: ['sum']}).reset_index()
    result.columns = ['token', 'OrderType', 'nSL_count', 'nSL_qty']

    pivot_temp_df = result.pivot(index=['token'], columns='OrderType', values=['nSL_count', 'nSL_qty']).reset_index()
    pivot_temp_df.columns = ['token', 'nBuySL_count', 'nSellSL_count', 'nBuySL_qty', 'nSellSL_qty']

    pivot_temp_df = pivot_temp_df.merge(SL_mean, on='token', how='left')

    pivot_temp_df = pivot_temp_df[(pivot_temp_df['nBuySL_count'] > 100) & (pivot_temp_df['nSellSL_count'] > 100)]

    pivot_temp_df['b_ratio'] = pivot_temp_df['nBuySL_qty'] / pivot_temp_df['nBuySL_qty_mean']
    pivot_temp_df['s_ratio'] = pivot_temp_df['nSellSL_qty'] / pivot_temp_df['nSellSL_qty_mean']

    pivot_temp_df['b_bs_ratio'] = pivot_temp_df['nBuySL_qty'] / pivot_temp_df['nSellSL_qty']
    pivot_temp_df['b_multiply_ratio'] = pivot_temp_df['b_ratio'] * pivot_temp_df['b_bs_ratio']

    pivot_temp_df['s_bs_ratio'] = pivot_temp_df['nSellSL_qty'] / pivot_temp_df['nBuySL_qty']
    pivot_temp_df['s_multiply_ratio'] = pivot_temp_df['s_ratio'] * pivot_temp_df['s_bs_ratio']

    pivot_temp_df = pivot_temp_df.round(0)

    stockmetadata_df = pd.read_csv(python_ankit_dir + '//StockMetadata.csv')
    pivot_temp_df = pivot_temp_df.merge(stockmetadata_df, on='token', how='left')
    pivot_temp_df.drop(['pdSectorInd', 'totalMarketCap', 'Series', 'Date_Updated', 'Sector', 'Industry', 'companyName'],
                       axis=1, inplace=True)

    cond = ((pivot_temp_df['b_ratio'] > 10) & (pivot_temp_df['MarketCap'] == 'LARGE')
            | (pivot_temp_df['b_ratio'] > 10) & (pivot_temp_df['MarketCap'] == 'Mid')
            | (pivot_temp_df['b_ratio'] > 10) & (pivot_temp_df['MarketCap'] == 'Small')
            | (pivot_temp_df['b_ratio'] > 10) & (pivot_temp_df['MarketCap'] == 'VSM'))
    pivot_temp_buy_df = pivot_temp_df  # [(cond)]
    pivot_temp_buy_df = pivot_temp_buy_df[
        ['Symbol', 'nBuySL_count', 'nSellSL_count', 'nBuySL_qty', 'nSellSL_qty', 'b_multiply_ratio', 'MarketCap',
         'token']]

    pivot_temp_buy_df = pivot_temp_buy_df.sort_values("b_multiply_ratio", ascending=False)
    pivot_temp_buy_df['buy/sell'] = pivot_temp_buy_df['nBuySL_qty'] / pivot_temp_buy_df['nSellSL_qty']

    cond = ((pivot_temp_df['s_ratio'] > 10) & (pivot_temp_df['MarketCap'] == 'LARGE')
            | (pivot_temp_df['s_ratio'] > 10) & (pivot_temp_df['MarketCap'] == 'Mid')
            | (pivot_temp_df['s_ratio'] > 10) & (pivot_temp_df['MarketCap'] == 'Small')
            | (pivot_temp_df['s_ratio'] > 10) & (pivot_temp_df['MarketCap'] == 'VSM'))
    pivot_temp_sell_df = pivot_temp_df  # [(cond)]

    pivot_temp_sell_df = pivot_temp_sell_df[
        ['Symbol', 'nBuySL_count', 'nSellSL_count', 'nBuySL_qty', 'nSellSL_qty', 's_multiply_ratio', 'MarketCap',
         'token']]
    pivot_temp_sell_df = pivot_temp_sell_df.sort_values("s_multiply_ratio", ascending=False)
    pivot_temp_sell_df['sell/buy'] = pivot_temp_sell_df['nSellSL_qty'] / pivot_temp_sell_df['nBuySL_qty']

    return pivot_temp_buy_df, pivot_temp_sell_df


def tradeWatch_historical():
    filename = f'''tradeWatch_historical.csv'''

    # filepath=os.path.join(tradeWatch_historical_file_path,filename)

    historical_df = pd.read_csv(filepath, header=None)[[0, 1, 2, 7]]
    historical_df[0] = pd.to_datetime(historical_df[0])

    y = np.expand_dims(historical_df[0].unique(), axis=1)
    row_df = pd.DataFrame(y).reset_index()
    row_df.rename(columns={'index': 'Row_number'}, inplace=True)
    final_df = historical_df.merge(row_df, on=0)
    # final_df.set_index('Row_number',inplace=True)
    max_count = final_df['Row_number'].max()
    final_df = final_df[final_df['Row_number'] > 0]

    merged_df_final = pd.DataFrame()
    max_df = final_df[final_df['Row_number'] == 1][[2, 7]]
    max_df.rename(columns={7: 'max'}, inplace=True)

    for i in range(max_count - 1):
        final_df_pre = final_df[final_df['Row_number'] == i + 1]
        merged_df = pd.merge(final_df_pre, max_df, on=2)

        merged_df['Flag'] = np.where(merged_df[7] > merged_df['max'], 1, 0)
        merged_df['max'] = np.where(merged_df['max'] >= merged_df[7], merged_df['max'], merged_df[7])
        max_df = merged_df[[2, 'max']]

        merged_df.rename(columns={0: 'Date', 2: 'Stock', 1: 'Token', 7: 'Qty'}, inplace=True)
        merged_df_final = pd.concat(
            [merged_df_final, merged_df[['Date', 'Token', 'Stock', 'Qty', 'Row_number', 'max', 'Flag']]])

    merged_df_final = merged_df_final[merged_df_final['Flag'] == 1]

    # merged_df_final.to_csv("max_progression_{}.csv".format(DATE_TODAY), index=False)
    merged_df_final.to_csv(aggregate_file_dir + '//report//max_progression.csv', index=None)
    merged_df_final_group = merged_df_final.groupby(['Stock']).agg({'Flag': ['sum']}).reset_index()
    merged_df_final_group.columns = ['Symbol', 'Count_maxpgr']
    # wb.sheets("max_prog").range("A1").options(index=False).value = merged_df_final_group.sort_values("Count_maxpgr",
    #                                                                                                  ascending=False)
    max_prog = merged_df_final_group.sort_values("Count_maxpgr", ascending=False)

    return max_prog


def watch_perm_trades_stream_op():
    division_factor = 375

    df_norman_trd = pd.read_csv(python_ankit_dir + '/Average.csv')
    df_norman_trd['normean_quantity'] = df_norman_trd['vol_norm_mean'] // division_factor  # mean_quantity
    df_norman_trd['normean_trd'] = df_norman_trd['trd_norm_mean'] // division_factor
    df_norman_trd = df_norman_trd[['Stock', 'normean_quantity', 'normean_trd']]
    df_norman_trd.rename(columns={'Stock': 'Symbol'}, inplace=True)

    df_5_groupby = pd.read_csv(filepath, header=None)  # keep same
    df_5_groupby.columns = ['Date', 'token', 'Symbol', 'row_count', 'sum_qty', 'nbuy_count', 'nsell_count',
                            'vwap_min',
                            'nbuy_qty', 'nsell_qty']
    df_5_groupby['Date'] = pd.to_datetime(df_5_groupby['Date'])

    OPENING_DATA_FILE = aggregate_file_dir + '/tradewatch/openingdata_{}.csv'.format(DATE_TODAY)
    open_data = pd.read_csv(OPENING_DATA_FILE, header=None)
    open_data.rename(columns={0: 'token', 1: 'o_price'}, inplace=True)

    df_6 = pd.merge(df_5_groupby, open_data, on=['token'])
    df_6 = pd.merge(df_6, df_norman_trd, on='Symbol')
    df_6 = df_6[
        ["Date", "token", "normean_quantity", "normean_trd", "nbuy_count", "nsell_count", "nbuy_qty", "nsell_qty",
         "row_count", "sum_qty", "o_price", "vwap_min", "Symbol"]]

    stockmetadata_df = pd.read_csv(python_ankit_dir + '/StockMetadata.csv')
    stockmetadata_df = stockmetadata_df[['Symbol', 'MarketCap']]
    df_6 = df_6.merge(stockmetadata_df, on='Symbol', how='left')

    df_6['r_qty'] = df_6['sum_qty'] // df_6['normean_quantity']
    df_6['r_trade'] = df_6['row_count'] // df_6['normean_trd']
    df_6['BUY'] = df_6['nbuy_qty'] // df_6['nsell_qty']
    df_6.dropna(subset=['Symbol'], inplace=True)

    # calc vwap
    ohlc_filepath = aggregate_file_dir + '/ohlc_{}.csv'.format(DATE_TODAY)
    df_ohlc = pd.read_csv(ohlc_filepath, header=None)[[0, 1, 6, 7]]
    df_ohlc[0] = pd.to_datetime(df_ohlc[0])
    df_ohlc[["vwap_all"]] = df_ohlc.groupby([1])[6].cumsum(axis=0) // df_ohlc.groupby([1])[7].cumsum(axis=0)

    df_ohlc = df_ohlc[[0, 1, "vwap_all"]]
    df_ohlc.columns = ['Date', 'token', 'vwap_all']
    df_6 = pd.merge(df_6, df_ohlc, on=['Date', 'token'])

    df_6_buy = df_6[df_6['vwap_min'] > df_6['vwap_all']]

    df_6_sell = df_6[df_6['vwap_min'] < df_6['vwap_all']]

    # 3 conditions row_count and o_price and qty > x * mean
    cond_b = ((df_6_buy['row_count'] >= 100) & (df_6_buy['vwap_min'] > df_6_buy['o_price'])
              & ((df_6_buy['sum_qty'] > 4 * df_6_buy['normean_quantity']) & (df_6_buy['MarketCap'] == 'LARGE')
                 | (df_6_buy['sum_qty'] > 4 * df_6_buy['normean_quantity']) & (df_6_buy['MarketCap'] == 'Mid')
                 | (df_6_buy['sum_qty'] > 8 * df_6_buy['normean_quantity']) & (df_6_buy['MarketCap'] == 'Small')
                 | (df_6_buy['sum_qty'] > 15 * df_6_buy['normean_quantity']) & (df_6_buy['MarketCap'] == 'VSM')))

    df_6_buy['repeat_b'] = np.where(cond_b, 1, 0)
    df_6_buy['repeat_bc'] = df_6_buy.groupby(["token"]).agg({'repeat_b': ['cumsum']})

    df_6_buy = df_6_buy.groupby(["token", "Symbol", "MarketCap"])
    df_6_buy = df_6_buy.tail(1)[["Date", "token", "Symbol", "MarketCap", "repeat_bc", "vwap_min", "vwap_all"]]

    cond_s = ((df_6_sell['row_count'] >= 100) & (df_6_sell['vwap_min'] < df_6_sell['o_price'])
              & ((df_6_sell['sum_qty'] > 4 * df_6_sell['normean_quantity']) & (df_6_sell['MarketCap'] == 'LARGE')
                 | (df_6_sell['sum_qty'] > 4 * df_6_sell['normean_quantity']) & (df_6_sell['MarketCap'] == 'Mid')
                 | (df_6_sell['sum_qty'] > 8 * df_6_sell['normean_quantity']) & (df_6_sell['MarketCap'] == 'Small')
                 | (df_6_sell['sum_qty'] > 15 * df_6_sell['normean_quantity']) & (df_6_sell['MarketCap'] == 'VSM')))

    df_6_sell['repeat_s'] = np.where(cond_s, 1, 0)
    df_6_sell['repeat_sc'] = df_6_sell.groupby(["token"]).agg({'repeat_s': ['cumsum']})
    df_6_sell = df_6_sell.groupby(["token", "Symbol", "MarketCap"])
    df_6_sell = df_6_sell.tail(1)[["Date", "token", "Symbol", "MarketCap", "repeat_sc", "vwap_min", "vwap_all"]]

    buy_new = df_6_buy.sort_values("repeat_bc", ascending=False)
    sell_new = df_6_sell.sort_values("repeat_sc", ascending=False)

    return buy_new, sell_new


def call():
    buy_new, sell_new = watch_perm_trades_stream_op()
    pivot_temp_buy_df, pivot_temp_sell_df = SL_Daily_test()
    max_prog = tradeWatch_historical()

    # merging dataframes buy
    df_merge_buy = pd.merge(buy_new, max_prog, on=['Symbol'], how='left')
    df_merge_buy = pd.merge(df_merge_buy, pivot_temp_buy_df, on=['Symbol', 'MarketCap', 'token'], how='left')

    # merging dataframes Sell
    df_merge_sell = pd.merge(sell_new, pivot_temp_sell_df, how='left')

    # dsr resistance and function call_dsr is in get_stream_data_by_token_dev
    df_buy = df_merge_buy[['token', 'Symbol']]
    df_sell = df_merge_sell[['token', 'Symbol']]
    df_buy = df_buy.head(1)
    df_sell = df_sell.head(1)
    df_dsr = pd.concat([df_buy, df_sell])
    df_dsr = (df_dsr.drop_duplicates(subset=['token']))
    print(df_dsr)
    call_dsr(df_dsr)

    # merging dsr resistance
    dsr_resistance = pd.read_csv(r'/home/workspace/aggregate/report/dsr_resistance.csv')
    dsr_resistance.drop(['token'], inplace=True, axis=1)
    dsr_resistance = dsr_resistance.rename(columns={"sellOrder": "token"})
    dsr_support = pd.read_csv(r'/home/workspace/aggregate/report/dsr_support.csv')
    dsr_support.drop(['token'], inplace=True, axis=1)
    dsr_support = dsr_support.rename(columns={"sellOrder": "token"})

    df_merge_buy = pd.merge(df_merge_buy, dsr_resistance, on=['token'], how='left')
    df_merge_sell = pd.merge(df_merge_sell, dsr_support, on=['token'], how='left')
    df_merge_buy.to_csv(aggregate_file_dir + '//report//trade_watch_merge_Report_Buy.csv', index=None)
    df_merge_sell.to_csv(aggregate_file_dir + '//report//trade_watch_merge_Report_Sell.csv', index=None)


if __name__ == '__main__':
    call()






