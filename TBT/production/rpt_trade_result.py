import pandas as pd
from collections import Counter
from datetime import datetime
import getpass
import numpy as np
from filepath import *
import datetime as dt



# Input from Excel
# if getpass.getuser() == 'ankit':
#     data_excel_file = python_ankit_dir + "\\Reporting_ankit.xlsm"
# if getpass.getuser() == 'VIJITR':
#     data_excel_file = python_ankit_dir + "\\Reporting_.xlsm"

# wb = xw.Book(data_excel_file)

# Change date for file read  and comment date from today
date = '20220104'
date = ''.join(str(datetime.today().date()).split('-'))
print(date)
pre_open_df = []

try:
    pre_open_df = pd.read_csv(aggregate_file_dir + '//preopen//preopen_Historical_' + date + '.csv')
    stockmetadata_df = pd.read_csv(python_ankit_dir + '//StockMetadata.csv')

    stockmetadata_df = stockmetadata_df[['Symbol', 'MarketCap', 'token']]
    pre_open_df = pre_open_df.merge(stockmetadata_df, on='token', how='left')

    cond = ((pre_open_df['Qty_ratio'] > 2) & (pre_open_df['MarketCap'] == 'LARGE')
            | (pre_open_df['Qty_ratio'] > 5) & (pre_open_df['MarketCap'] == 'Small')
            | (pre_open_df['Qty_ratio'] > 5) & (pre_open_df['MarketCap'] == 'Mid')
            | (pre_open_df['Qty_ratio'] > 20) & (pre_open_df['MarketCap'] == 'VSM'))

    # wb.sheets("pre_open").range("A1").options(index=False).value = pre_open_df[cond].sort_values(
    #     by=['MarketCap', 'Qty_ratio'], ascending=False)
    # wb.sheets("pre_open_banknifty").range("A1").options(index=False).value = pre_open_df[
    #     pre_open_df['token'].isin(['1333', '4963', '1922', '5900', '5258', '3045', '2263'])]
    # wb.sheets("pre_open_nifty").range("A1").options(index=False).value = pre_open_df[
    #     pre_open_df['token'].isin(['2885', '1333', '1594', '1330', '4963', '11536', '1922', '1394', '1660', '11483'])]

    pre_open_csv = pre_open_df[cond].sort_values(by=['MarketCap', 'Qty_ratio'], ascending=False)
    pre_open_banknifty_csv = pre_open_df[
        pre_open_df['token'].isin(['1333', '4963', '1922', '5900', '5258', '3045', '2263'])]
    pre_open_nifty_csv = pre_open_df[
        pre_open_df['token'].isin(['2885', '1333', '1594', '1330', '4963', '11536', '1922', '1394', '1660', '11483'])]
    pre_open_csv.to_csv(aggregate_file_dir + '//report//trade_result_pre_open.csv', index=None)
    pre_open_banknifty_csv.to_csv(aggregate_file_dir + '//report//trade_result_pre_open_banknifty.csv', index=None)
    pre_open_nifty_csv.to_csv(aggregate_file_dir + '//report//trade_result_pre_open_nifty.csv', index=None)
except:
    pass

df = pd.read_csv(aggregate_file_dir + '//trade_result_final_' + date + '.csv', header=None)
open_df = pd.read_csv(aggregate_file_dir + '//tradewatch//openingdata_' + date + '.csv', header=None)
# open_ls_df = pd.read_csv('E:\\DUMPER\\tradewatch\\metadata\\openingdata_ls_' + date + '.csv', header = None)
# open_df = open_df.append(open_ls_df)

todaydate = pd.to_datetime(df[0].max())
todaydate = todaydate.strftime('%d-%m-%Y')

d = Counter(list(df[2]))
temp = pd.DataFrame.from_dict(d, orient='index').reset_index()

df['qty_ratio'] = df[4] // df[8]
df = df.merge(open_df, left_on=1, right_on=0, how='outer')
df['B_S'] = np.where((df['1_y'] <= df[7]), 'B', 'S')
df.drop([1, '0_y'], axis='columns', inplace=True)

output = df.groupby([2, 'B_S']).agg({2: ['count'], 4: ['mean'], 8: ['mean'], 9: ['mean']}).reset_index()
output.columns = ['Symbol', 'B_S', 'Count', 'Qty', 'Mean_Qty_1min', 'Mean_trds_1min']
output['qty_ratio'] = output['Qty'] // output['Mean_Qty_1min']

output_b_df = output[(output['B_S'] == 'B') & (output['Count'] > 5)]
output_b_df = output_b_df.sort_values(by=['Count'], ascending=False).reset_index(drop=True)

output_s_df = output[(output['B_S'] == 'S') & (output['Count'] > 5)]
output_s_df = output_s_df.sort_values(by=['Count'], ascending=False).reset_index(drop=True)

output_b_df10 = output_b_df.head(10)
output_s_df10 = output_s_df.head(10)

output_b_s = output_b_df10.merge(output_s_df10, on='Symbol', how='outer')
output_b_s = output_b_s.sort_values(by=['Count_x'], ascending=False).reset_index(drop=True)

# wb.sheets("Report").range("A1").options(index=False).value = output_b_s
# wb.sheets("Raw_Data").range("A2").options(index=False, header=False).value = df
df.to_csv(aggregate_file_dir + '//report//trade_result_Raw_Data.csv', index=None)


# # wb.sheets("Buy").range("A1").options(index=False).value = output_b_df
# # wb.sheets("Sell").range("A1").options(index=False).value = output_s_df

output['date'] = todaydate
temp_df = pd.read_csv(trade_watch_dir + '/historical_data.csv')
temp_df['date'] = pd.to_datetime(temp_df['date'], format='%d-%m-%Y')
temp_df = temp_df[temp_df['date'] != todaydate]
output['date'] = pd.to_datetime(output['date'], format='%d-%m-%Y')
temp_df = temp_df.append(output)

output = output.sort_values(by=['Count'], ascending=False).reset_index(drop=True)

output_top_50_df = output.head(50)


cond = temp_df['Symbol'].isin(output_top_50_df['Symbol'])
pivot_temp_df = temp_df[cond]

pivot_temp_df = pivot_temp_df[pivot_temp_df['Count'] > 10]
pivot_temp_df = pivot_temp_df.sort_values(by=['Count'], ascending=False)
pivot_temp_df['date'] = pd.to_datetime(pivot_temp_df['date'])
# pivot_temp_df['date'] = pd.to_datetime(pivot_temp_df['date']).dt.strftime('%d-%m-%Y')
pivot_temp_df = pivot_temp_df.pivot(index=['Symbol', 'B_S'], columns='date', values='Count')

pivot_temp_df = pivot_temp_df.sort_index(ascending=False, axis=1)
# wb.sheets("Previous").range("A1").options().value = pivot_temp_df
pivot_temp_df.to_csv(aggregate_file_dir + '//report//trade_result_Previous.csv')

timesleep = datetime.now().strftime('%H:%M')

if timesleep > "15:29":
    temp_df['date'] = temp_df['date'].dt.strftime('%d-%m-%Y')
    # temp_df = temp_df.sort_values(by = ['date'], ascending = False, axis = 0)
    temp_df.to_csv(trade_watch_dir + '//historical_data.csv', index=False)


