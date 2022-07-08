import subprocess
from datetime import datetime
import getpass
import xlwings as xw
import pandas as pd
import numpy as np
from get_stream_data_by_token_filepath import *

#Input from Excel
if getpass.getuser() == 'ankit':
    data_excel_file = python_ankit_dir  + "\\Stream_ankit.xlsm"
if getpass.getuser() == 'VIJITR':
    data_excel_file = python_ankit_dir  + "\\Stream.xlsm"

wb = xw.Book(data_excel_file)

stream= str(wb.sheets("Input").range('B1').options(index=False).value)
token= str(wb.sheets("Input").range('B2').options(index=False).value)
date = ''.join(str(datetime.today().date()).split('-'))

stream_path = input_file_dir  + "\\DUMP_" + str(date) + "_07300" + stream + ".csv_CLEAN"
drop_path = dumper_file_dir + "\\Stream_by_token\metadata\\TBT_" + str(date)+"_" + token + ".csv"

subprocess.call(("findstr {} {} > {}").format( "," +token +"," , stream_path, drop_path+"_str"), shell=True)

with open(drop_path,'w') as fd:
    with open(drop_path+"_str",'r') as read_obj:
        for line in read_obj:
            first = line.split(',')
            if ((first[3] == token) or (first[4] == token)):
                fd.write(line)


subprocess.call("del {}".format(drop_path+"_str"), shell=True)


import datetime
from datetime import datetime
from datetime import datetime as dt


#pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', None)


###take input from excel cell value and round off the datetime in df_live_stream and df_stream_token_traded
#data_excel_file = "C:\\Python_Ankit\\Stream.xlsm"
#wb = xw.Book(data_excel_file)

scriptcode = int(wb.sheets("Input").range('B2').options(index=False).value)
minround = int(wb.sheets("Input").range('B3').options(index=False).value)

# Read CSV into dataframe
script_filename = dumper_file_dir + "\\Stream_by_token\metadata\\TBT_{0}_".format(datetime.now().strftime("%Y%m%d")) + str(
    scriptcode) + ".csv"
print(script_filename)
df_stream_token = pd.read_csv(script_filename, header=None)

# convert Unix_epoch to datetime
# df_stream_token['datetime'] =  pd.to_datetime('1980-01-01 00:00:00') + pd.to_timedelta(df_stream_token[6]).dt.floor("S")
# drop columns not needed
df_stream_token.rename(
    columns={0: "uniqueRow", 1: "orderType", 2: "buyOrder", 3: "sellOrder", 4: "token", 5: "pricePaisa", 6: "quantity",
             7: "datetime"}, inplace=True)

df_stream_token["datetime"] = pd.to_datetime(df_stream_token['datetime'])

df_stream_token_new = df_stream_token[df_stream_token['orderType'] == "N"]


#SL orders
min_value = float('-inf')
order_id = []
for index, row in df_stream_token_new.iterrows():
    if row['buyOrder'] > min_value:
        min_value = row['buyOrder']
    else:
        order_id.append(row['buyOrder'])


df_stream_token["buyOrder"] = df_stream_token['buyOrder'].apply(str)
df_stream_token["sellOrder"] = df_stream_token['sellOrder'].apply(str)

df_stream_token_new = df_stream_token[df_stream_token['orderType'] == "N"]
df_stream_token_traded = df_stream_token[df_stream_token['orderType'] == "T"]


# Add missing new buy and sell order
df_buy_new_missing = df_stream_token_traded[~df_stream_token_traded['buyOrder'].isin(df_stream_token_new['buyOrder'])]

df_buy_new_missing['sellOrder'] = df_buy_new_missing['token']
df_buy_new_missing[['token', 'orderType']] = ['B', 'N']

df_sell_new_missing = df_stream_token_traded[~df_stream_token_traded['sellOrder'].isin(df_stream_token_new['buyOrder'])]

df_sell_new_missing['buyOrder'] = df_sell_new_missing['sellOrder']
df_sell_new_missing['sellOrder'] = df_sell_new_missing['token']
df_sell_new_missing[['token', 'orderType']] = ['S', 'N']

# duplicate new order
df_stream_token_new_groupby_buy = df_stream_token_new.groupby(['buyOrder'])
df_stream_token_new_buy_count = df_stream_token_new_groupby_buy[['orderType']].count()
df_stream_token_new_groupby_buy = []
df_stream_token_new_buy_count.rename(columns={"orderType": "count"}, inplace=True)

df_stream_token_new_dup = df_stream_token_new_buy_count[df_stream_token_new_buy_count['count'] > 1]
df_stream_token_new_dup = df_stream_token_new_dup.reset_index()

cond = df_stream_token_new['buyOrder'].isin(df_stream_token_new_dup['buyOrder'])
df_stream_token_new.drop(df_stream_token_new[cond].index, inplace=True)

cond = df_stream_token['buyOrder'].isin(df_stream_token_new_dup['buyOrder'])
df_stream_token_dups = df_stream_token[cond]  # Take new  dups into seperate df
df_stream_token.drop(df_stream_token[cond].index, inplace=True)

df_stream_token_cancelled = df_stream_token[df_stream_token['orderType'] == "X"]

df_cancelled_new_missing = df_stream_token_cancelled[
    ~df_stream_token_cancelled['buyOrder'].isin(df_stream_token_new['buyOrder'])]

df_cancelled_new_missing["orderType"].replace({"X": "N"}, inplace=True)

df_missing = pd.concat([df_buy_new_missing, df_sell_new_missing])
df_missing = pd.concat([df_missing, df_cancelled_new_missing])
df_missing = df_missing[(df_missing['buyOrder'] != '0')]


if df_missing.empty:
    print('')
else:
    df_missing = df_missing.groupby(['buyOrder', 'sellOrder']).agg({'uniqueRow': 'min', 'orderType': 'max', 'token': 'max', 'pricePaisa': 'mean', 'quantity': 'sum',
     'datetime': 'max'})
    df_missing = df_missing.reset_index(['sellOrder', 'buyOrder'])

df_stream_token_new = pd.concat([df_missing, df_stream_token_new])

df_missing = []

options = ['0']
df_buy_zero = df_stream_token_traded[df_stream_token_traded['buyOrder'].isin(options)]
df_buy_zero['buyquantity'] = df_buy_zero['quantity']
df_buy_zero['sellOrder'] = df_buy_zero['token']
df_buy_zero['b_avg_price'] = df_buy_zero['pricePaisa']
df_buy_zero[['token', 'orderType']] = ['B', 'N']

df_sell_zero = df_stream_token_traded[df_stream_token_traded['sellOrder'].isin(options)]
df_sell_zero['buyOrder'] = df_sell_zero['sellOrder']
df_sell_zero['sellOrder'] = df_sell_zero['token']
df_sell_zero[['token', 'orderType']] = ['S', 'N']
df_sell_zero['sellquantity'] = df_sell_zero['quantity']
df_sell_zero['s_avg_price'] = df_sell_zero['pricePaisa']

df_zero = pd.concat([df_buy_zero, df_sell_zero])
df_sell_zero['buyOrder'] = df_sell_zero['uniqueRow']


df_stream_token = []
# duplicate cancel order
df_stream_token_cancel_groupby_buy = df_stream_token_cancelled.groupby(['buyOrder'])
df_stream_token_cancel_buy_count = df_stream_token_cancel_groupby_buy[['orderType']].count()
df_stream_token_cancel_buy_count.rename(columns={"orderType": "count"}, inplace=True)
df_stream_token_cancel_dup = df_stream_token_cancel_buy_count[df_stream_token_cancel_buy_count['count'] > 1]

df_stream_token_cancel_dup = df_stream_token_cancel_dup.reset_index()

cond = df_stream_token_cancelled['buyOrder'].isin(df_stream_token_cancel_dup['buyOrder'])
df_stream_token_cancelled_dups = df_stream_token_cancelled[cond]  # Take cancel dups into seperate df
df_stream_token_cancelled.drop(df_stream_token_cancelled[cond].index, inplace=True)
# 1100000000444745

df_stream_token_traded_big_quantity = df_stream_token_traded.copy()
df_stream_token_traded_big_quantity["uniqueRow"] = df_stream_token_traded_big_quantity['uniqueRow'].apply(str)

df_stream_token_traded_big_quantity['buyOrder'] = np.where( df_stream_token_traded_big_quantity['buyOrder'] == '0',  df_stream_token_traded_big_quantity['uniqueRow'],  df_stream_token_traded_big_quantity['buyOrder'])
df_stream_token_traded_big_quantity['sellOrder'] = np.where( df_stream_token_traded_big_quantity['sellOrder'] == '0',  df_stream_token_traded_big_quantity['uniqueRow'],  df_stream_token_traded_big_quantity['sellOrder'])


df_stream_token_traded_groupby_buy = df_stream_token_traded_big_quantity.groupby(['buyOrder'])
#df_stream_token_traded_buy_sum = df_stream_token_traded_groupby_buy[['quantity']].sum()
df_stream_token_traded_buy_sum = df_stream_token_traded_groupby_buy.agg({'quantity':sum,'pricePaisa':'mean','datetime':max})
df_stream_token_traded_groupby_buy=[]
df_stream_token_traded_buy_sum.rename(columns={"quantity": "buyquantity","pricePaisa":"b_avg_price","datetime":"b_datetime"}, inplace = True)


df_stream_token_traded_groupby_sell = df_stream_token_traded_big_quantity.groupby(['sellOrder'])
df_stream_token_traded_sell_sum = df_stream_token_traded_groupby_sell.agg({'quantity':sum,'pricePaisa':"mean",'datetime':max})
df_stream_token_traded_groupby_sell=[]
df_stream_token_traded_sell_sum.rename(columns={"quantity": "sellquantity","pricePaisa":"s_avg_price","datetime":"s_datetime"}, inplace = True)
df_stream_token_traded_sell_sum.index.name='buyOrder'

df_stream_token_new.set_index(['buyOrder'], inplace=True)
df_stream_token_cancelled.set_index(['buyOrder'], inplace=True)
df_stream_token_cancelled_new = pd.DataFrame()
df_stream_token_cancelled_new[["cancellquantity","Cancelldatetime"]] = df_stream_token_cancelled[["quantity", "datetime"]]

df_stream_token_cancelled = []
# Output Excel
sheet_oi_single = wb.sheets("big_quantity")

sheet_oi_single.range("A1").options().value = df_stream_token_traded_buy_sum[
    df_stream_token_traded_buy_sum['buyquantity'] > 1000].sort_values("buyquantity", ascending=False)
sheet_oi_single.range("E1").options().value = df_stream_token_traded_sell_sum[
    df_stream_token_traded_sell_sum['sellquantity'] > 1000].sort_values("sellquantity", ascending=False)

# wb.sheets("New_dups").range("A1").options().value = df_stream_token_dups
# wb.sheets("Cancel_dups").range("A1").options().value = df_stream_token_cancelled_dups
df_stream_token_new = df_stream_token_new.reset_index()

##check is null for each df#####


df_live_stream = df_stream_token_new.merge(df_stream_token_cancelled_new, how='outer', on='buyOrder')

df_live_stream = df_live_stream.merge(df_stream_token_traded_buy_sum, how='outer', on='buyOrder')

df_stream_token_traded_sell_sum = df_stream_token_traded_sell_sum.reset_index()

df_live_stream = df_live_stream.merge(df_stream_token_traded_sell_sum, how='outer', on='buyOrder')

#df_live_stream = df_live_stream[df_live_stream['buyOrder'] != 0]
df_live_stream = df_live_stream[df_live_stream['buyOrder'].astype(str).str.match(r'^(\d{10,})$')]
df_live_stream = pd.concat([df_live_stream, df_zero])

df_stream_token_traded_buy_sum = []
df_stream_token_traded_sell_sum = []
df_stream_token_cancelled_new = []

#
df_live_stream['Round_Time'] = df_live_stream['datetime'].dt.floor(str(minround) + "min")
# df_live_stream['Round_Time'] = pd.to_datetime(df_live_stream['Round_Time1']).dt.time
# df_live_stream = df_live_stream.drop(columns='Round_Time1')

df_stream_token_traded['Round_Time'] = df_stream_token_traded['datetime'].dt.floor(str(minround) + "min")
# df_stream_token_traded['Round_Time'] = pd.to_datetime(df_stream_token_traded['Round_Time1']).dt.time
# df_stream_token_traded = df_stream_token_traded.drop(columns='Round_Time1')

###round off pricePaisa column
df_live_stream['Price_Round'] = df_live_stream['pricePaisa'].round(-2)

###column isDisclosed in all_trades
df_live_stream['IsDisclosed'] = np.where(((df_live_stream.buyquantity) > (df_live_stream.quantity)) | (
            (df_live_stream.sellquantity) > (df_live_stream.quantity)), np.where(df_live_stream.buyquantity.isnull(), (
            df_live_stream.sellquantity / df_live_stream.quantity), (
                                                                                             df_live_stream.buyquantity / df_live_stream.quantity)),
                                         0)

###column Status in all_trades

df_live_stream['quantity'] = df_live_stream['quantity'].fillna(0)
df_live_stream['buyquantity'] = df_live_stream['buyquantity'].fillna(0)
df_live_stream['sellquantity'] = df_live_stream['sellquantity'].fillna(0)
df_live_stream['cancellquantity'] = df_live_stream['cancellquantity'].fillna(0)
df_live_stream['bs'] = (df_live_stream['buyquantity'] + df_live_stream['sellquantity'])

df_live_stream['Status'] = np.where((df_live_stream.buyquantity) == (df_live_stream.sellquantity),
                                    np.where(df_live_stream.buyquantity == 0,
                                             np.where(((df_live_stream.cancellquantity) > 0), 'Cancel', 'Pending'),
                                             np.nan), 'Traded')

###column Substatus
df_live_stream['SubStatus'] = np.where(
    (df_live_stream.cancellquantity != 0) & (df_live_stream.buyquantity != df_live_stream.sellquantity),
    'Partial Cancel', np.where(
        (df_live_stream.bs < df_live_stream.quantity) & (df_live_stream.buyquantity != df_live_stream.sellquantity),
        'Partial Pending', 0))

df_live_stream = df_live_stream.drop(columns='bs')

#### groupby and merge task
df1 = df_stream_token_traded.groupby('Round_Time').agg(
    {'orderType': ['count'], 'pricePaisa': ['max'], 'quantity': ['sum']})
# print(df1.head())
df_sell = df_live_stream[df_live_stream['token'] == 'S']
df_buy = df_live_stream[df_live_stream['token'] == 'B']
# print(df_buy.head(3))
df_buy1 = df_buy.groupby('Round_Time').agg({'buyOrder': ['count'], 'quantity': ['sum']})
df_sell1 = df_sell.groupby('Round_Time').agg({'sellOrder': ['count'], 'quantity': ['sum']})
df2 = pd.merge(df_buy1,df_sell1, on="Round_Time", how='inner')

df2.rename(columns={'quantity_x': 'sum of buyquantity', 'quantity_y': 'sum of sellquantity'}, inplace=True)
# print(df2.head())
df_final = pd.merge(df1, df2, on="Round_Time", how='inner')

### errorcode
df_stream_token_new = df_stream_token_new[df_stream_token_new['datetime'].dt.strftime('%H:%M') > "09:14"]
df_stream_token_new = df_stream_token_new.reset_index(drop=False)


df_live_stream['buyOrder1'] = df_live_stream['buyOrder'].astype(float)
df_live_stream_error = df_live_stream[df_live_stream['buyOrder1'].isin(order_id)]
df_live_stream_error = df_live_stream_error.drop(columns='buyOrder1')

df_live_stream.sort_values(by = ['uniqueRow'], inplace = True)

df_live_stream = df_live_stream.drop(columns='buyOrder1')
df_live_stream = df_live_stream.drop(columns='uniqueRow')
df_stream_token_traded = df_stream_token_traded.drop(columns='uniqueRow')
df_live_stream_error = df_live_stream_error.drop(columns='uniqueRow')

# #### saving the outputs to excel sheet
try:
    wb.sheets("SL").range("A1").options(index=False).value = df_live_stream_error[df_live_stream_error['datetime'].dt.strftime('%H:%M') > "09:14"]
    wb.sheets("Traded").range("A1").options(index=False).value = df_stream_token_traded
    wb.sheets("All_trades").range("A1").options(index=False).value = df_live_stream
    wb.sheets("Num_trades").range("A1").options().value = df_final

    # wb.sheets("Df_live").range("A1").options(index=False).value = df_live
except ValueError as V:
    print("Error:", V)
