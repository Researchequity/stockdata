# Create Pre open mean file of all previous day file
import pandas as pd
from glob import glob
from filepath import *
import getpass
from datetime import datetime

SL_all_df = pd.DataFrame()
date = ''.join(str(datetime.today().date()).split('-'))
print(date)
# Input from Excel
# if getpass.getuser() == 'ankit':
#     data_excel_file = python_ankit_dir + "\\Reporting_ankit.xlsm"
# if getpass.getuser() == 'VIJITR':
#     data_excel_file = python_ankit_dir + "\\Reporting_.xlsm"
#
# wb = xw.Book(data_excel_file)

SL_mean = pd.read_csv(aggregate_file_dir + "sl//slaverage.csv")

for file in glob(aggregate_file_dir + '/sl/scan//sl_{}_*.csv'.format(date)):
    print(file)
    SL_df = pd.read_csv(file, header=None)
    SL_all_df = pd.concat([SL_all_df, SL_df])

result = SL_all_df.groupby([3, 4]).agg({0: ['count'], 6: ['sum']}).reset_index()
result.columns = ['token', 'OrderType', 'nSL_count', 'nSL_qty']

pivot_temp_df = result.pivot(index=['token'], columns='OrderType', values=['nSL_count', 'nSL_qty']).reset_index()
pivot_temp_df.columns = ['token', 'nBuySL_count', 'nSellSL_count', 'nBuySL_qty', 'nSellSL_qty']

pivot_temp_df = pivot_temp_df.merge(SL_mean, on='token', how='left')

pivot_temp_df = pivot_temp_df[(pivot_temp_df['nBuySL_count'] > 100) & (pivot_temp_df['nSellSL_count'] > 100)]
# nBuySL_count_mean,nSellSL_count_mean,nBuySL_qty_mean,nSellSL_qty_mean

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
    ['Symbol', 'nBuySL_count', 'nSellSL_count', 'nBuySL_qty', 'nSellSL_qty', 'b_multiply_ratio', 'MarketCap', 'token']]

# wb.sheets("SL_buy").range("A1").options(index=False).value = pivot_temp_buy_df.sort_values("b_multiply_ratio",
#                                                                                            ascending=False)
df_csv = pivot_temp_buy_df.sort_values("b_multiply_ratio", ascending=False)
df_csv.to_csv(aggregate_file_dir + '//report//SL_Daily_SL_buy.csv', index=None)

cond = ((pivot_temp_df['s_ratio'] > 10) & (pivot_temp_df['MarketCap'] == 'LARGE')
        | (pivot_temp_df['s_ratio'] > 10) & (pivot_temp_df['MarketCap'] == 'Mid')
        | (pivot_temp_df['s_ratio'] > 10) & (pivot_temp_df['MarketCap'] == 'Small')
        | (pivot_temp_df['s_ratio'] > 10) & (pivot_temp_df['MarketCap'] == 'VSM'))
pivot_temp_sell_df = pivot_temp_df  # [(cond)]

pivot_temp_sell_df = pivot_temp_sell_df[
    ['Symbol', 'nBuySL_count', 'nSellSL_count', 'nBuySL_qty', 'nSellSL_qty', 's_multiply_ratio', 'MarketCap', 'token']]
# wb.sheets("SL_sell").range("A1").options(index=False).value = pivot_temp_sell_df.sort_values("s_multiply_ratio",
#                                                                                              ascending=False)
df_sell_csv = pivot_temp_sell_df.sort_values("s_multiply_ratio", ascending=False)
df_sell_csv.to_csv(aggregate_file_dir + '//report//SL_Daily_SL_sell.csv', index=None)

