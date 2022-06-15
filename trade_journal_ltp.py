import time
import pandas as pd
from filepath import *
import numpy as np
import requests
from time import sleep
import xlwings as xw
import threading
import datetime
import sys

pd.set_option('display.max_columns', None)

#name = sys.argv[1]
name = 'gaurav'

if name == 'rohan':
    loc = r"D:\Program\stockdata\Profit_Loss_rohan.xlsm"
elif name == 'gaurav':
    loc = r'\\192.168.41.190\gaurav\Profit_Loss_gaurav.xlsm'
elif name == 'chayan':
    loc = r'\\192.168.41.190\chayan\Profit_Loss_chayan.xlsm'
elif name == 'ankit':
    loc = r'\\192.168.41.190\ankit\Profit_Loss_ankit.xlsm'
elif name == 'report':
    loc = r'\\192.168.41.190\report\Profit_Loss_report.xlsm'
workbook = xw.Book(loc)
#workbook = xw.Book(METADATA_DIR + '\\' + 'Profit_Loss_chayan.xlsm')

threads = []
delivery_position_all_list = []
del_all_bse = []
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9", "Accept-Encoding": "gzip, deflate, br"}


def start_thread_bse(element):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; rv:82.0) Gecko/20100101 Firefox/82.0"}
    s = requests.session()
    try:
        print(element)

        url = "https://api.bseindia.com/BseIndiaAPI/api/StockTrading/w?flag=&quotetype=EQ&scripcode={}".format(
            int(element))
        r = s.get(url, headers=headers).json()

        url = "https://api.bseindia.com/BseIndiaAPI/api/getScripHeaderData/w?Debtflag=&scripcode={}&seriesid=".format(
            int(element))
        p = s.get(url, headers=headers).json()
        meta_trade_df = pd.DataFrame([r['WAP']])
        meta_trade_df['vwap'] = r['WAP']
        meta_trade_df['lastPrice'] = p['CurrRate']['LTP']
        meta_trade_df['pChange'] = p['CurrRate']['PcChg']
        meta_trade_df["quantityTraded"] = 0
        meta_trade_df["deliveryQuantity"] = 0
        meta_trade_df["deliveryToTradedQuantity"] = 0
        meta_trade_df.drop([0], inplace=True, axis=1)
        meta_trade_df['secWiseDelPosDate'] = datetime.datetime.today()
        meta_trade_df['Stock'] = int(element)
        # print(meta_trade_df)

        meta_trade_df = meta_trade_df.copy()

        del_all_bse.append(meta_trade_df.to_dict('records'))

    except Exception as e:
        print(element, "is wrong, error", e)


def start_thread(Stock, index, s):
    print(Stock)
    try:
        url = 'https://www.nseindia.com/api/quote-equity?symbol=' + Stock.replace('&', '%26')
        r_stock = s.get(url, headers=headers).json()
        df_price = pd.DataFrame([r_stock['priceInfo']])
        df_price = df_price[['lastPrice', 'pChange', 'vwap']]
        df_price['Stock'] = Stock
        df_price['secWiseDelPosDate'] = [r_stock['metadata']['lastUpdateTime']]
        df_price['quantityTraded'] = 0
        df_price['deliveryQuantity'] = 0
        df_price['deliveryToTradedQuantity'] = 0
        delivery_position_all_list.append(df_price.to_dict('records'))
    except Exception as e:
        print(e)


def bse_details(stock_bse):
    index = 0
    for Symbol in stock_bse:
        t1 = threading.Thread(target=start_thread_bse, args=Symbol)
        t1.start()
        index = index + 1
        threads.append(t1)
        if index % 100 == 0:
            sleep(10)

    for process in threads:
        process.join()


def nse_details(stock_nse):
    s = requests.session()
    s.get('https://www.nseindia.com/get-quotes/derivatives?symbol=' + 'NIFTY', headers=headers)
    r = s.get('https://www.nseindia.com/api/quote-equity?symbol=INFY&section=trade_info', headers=headers).json()
    print("r is the value",r)
    index = 0
    for Symbol in stock_nse:
        t1 = threading.Thread(target=start_thread, args=(Symbol, index, s))
        t1.start()
        index = index + 1
        threads.append(t1)
        if index % 30 == 0:
            sleep(1)
        if index % 200 == 0:
            s = requests.session()
            s.get('https://www.nseindia.com/get-quotes/derivatives?symbol=' + 'NIFTY', headers=headers)

    for process in threads:
        process.join()


def PNF_transaction(buy_df, sell_df):
    avg_df = pd.DataFrame()
    while len(buy_df[buy_df['Active'] >= 0]) > 0:
        buy_temp = buy_df[buy_df['Active'] >= 0].head(1)
        print("starting buy df ", buy_temp)
        buy_temp_idx = buy_temp.index.values[0]
        sell_temp = sell_df[sell_df['Active'] >= 0].head(1)
        if len(sell_temp) > 0:
            sell_temp_idx = sell_temp.head(1).index.values[0]
            if (sell_temp['Sell_Qty'].item() == buy_temp['Buy_Qty'].item()):
                sell_df.drop([sell_temp_idx], inplace=True)
                buy_df.drop([buy_temp_idx], inplace=True)

                buy_temp = buy_temp.reset_index(drop=True)
                sell_temp = sell_temp.reset_index(drop=True)
                equal_df = pd.concat([buy_temp.iloc[0:1, ], sell_temp.iloc[0:1, ]], axis=1)
                avg_df = pd.concat([avg_df, equal_df])

            elif (sell_temp['Sell_Qty'].item() < buy_temp['Buy_Qty'].item()):
                buy_qty_rem = buy_temp.loc[buy_temp_idx, 'Buy_Qty'] - sell_temp.loc[sell_temp_idx, 'Sell_Qty']

                buy_temp.loc[buy_temp_idx, 'Buy_Qty'] = sell_temp.loc[sell_temp_idx, 'Sell_Qty']
                buy_temp = buy_temp.reset_index(drop=True)
                sell_temp = sell_temp.reset_index(drop=True)

                gtr_df = pd.concat([buy_temp.iloc[0:1, ], sell_temp.iloc[0:1, ]], axis=1)
                avg_df = pd.concat([avg_df, gtr_df])
                sell_df.drop([sell_temp_idx], inplace=True)
                buy_df.loc[buy_temp_idx, 'Buy_Qty'] = buy_qty_rem

            elif (sell_temp['Sell_Qty'].item() > buy_temp['Buy_Qty'].item()):

                sell_qty_rem = sell_temp.loc[sell_temp_idx, 'Sell_Qty'] - buy_temp.loc[buy_temp_idx, 'Buy_Qty']
                sell_temp.loc[sell_temp_idx, 'Sell_Qty'] = buy_temp.loc[buy_temp_idx, 'Buy_Qty']
                buy_temp = buy_temp.reset_index(drop=True)
                sell_temp = sell_temp.reset_index(drop=True)

                less_df = pd.concat([buy_temp.iloc[0:1, ], sell_temp.iloc[0:1, ]], axis=1)
                avg_df = pd.concat([avg_df, less_df])
                buy_df.drop([buy_temp_idx], inplace=True)

                sell_df.loc[sell_temp_idx, 'Sell_Qty'] = sell_qty_rem
        else:
            buy_df.loc[buy_temp_idx, 'Active'] = -1
            empty_df = pd.concat([buy_temp.iloc[0:1, ], sell_df.iloc[0:1, ]], axis=1)
            avg_df = pd.concat([avg_df, empty_df])

    sellvolt = sell_df[sell_df['Active'] >= 0].head(1)

    if (len(sellvolt) > 0):
        volt = avg_df[['Exchange', 'Symbol', 'Scrip_Name']].head(1)
        buy_volt = buy_df[buy_df['Active'] >= 0].head(1)
        buy_volt = buy_volt.append(volt).head(1)

        sellvolt_inx = sellvolt.index.values[0]
        sell_df.loc[sellvolt_inx, 'Active'] = -1
        buy_volt = buy_volt.reset_index(drop=True)
        sellvolt = sellvolt.reset_index(drop=True)
        sell_empty_df = pd.concat([buy_volt.iloc[0:1, ], sellvolt.iloc[0:1, ]], axis=1)
        avg_df = pd.concat([avg_df, sell_empty_df])
    return avg_df


def Profit_And_Loss():
    df=pd.read_excel(loc,sheet_name='All_Delivery_Position')
    df = df[['Scrip Name ', 'Symbol ', 'Exchange ', 'Buy Qty ', 'Sell Qty ', 'Buy Val ', 'Sell Val ',
             'Buy Avg. ', 'Sell Avg. ']]
    df = df[df['Exchange '] != 0]
    df.drop(df.columns[df.columns.str.contains('unnamed', case=False)], axis=1, inplace=True)

    buy_df = df[['Exchange ', 'Symbol ', 'Scrip Name ', 'Buy Qty ', 'Buy Val ', 'Buy Avg. ']]
    buy_df.rename(columns={'Exchange ': 'Exchange',  'Scrip Name ': 'Scrip_Name', 'Symbol ': 'Symbol',
                 'Buy Qty ': 'Buy_Qty', 'Buy Val ': 'Buy_Val', 'Buy Avg. ': 'Buy_Avg'}, inplace=True)
    buy_df['TranType'] = 'Buy'
    buy_df['Active'] = 0
    buy_df = buy_df[buy_df['Buy_Qty'] > 0]
    buy_df.reset_index(inplace=True)

    sell_df = df[['Symbol ', 'Sell Qty ', 'Sell Val ', 'Sell Avg. ']]
    sell_df.rename(columns={'Symbol ': 'Sell_Symbol','Sell Qty ': 'Sell_Qty', 'Sell Val ': 'Sell_Val', 'Sell Avg. ': 'Sell_Avg'}, inplace=True)
    sell_df['TranType'] = 'Sell'
    sell_df['Active'] = 0
    sell_df = sell_df[sell_df['Sell_Qty'] > 0]
    sell_df.reset_index(inplace=True)


    df_merge=pd.DataFrame()
    uniqueValues = df['Symbol '].unique().tolist()
    for symbol in uniqueValues:
        buy_data = buy_df[buy_df['Symbol'] == symbol]
        sell_data = sell_df[sell_df['Sell_Symbol'] == symbol]
        buy_data = buy_data.reset_index(drop=True)
        sell_data = sell_data.reset_index(drop=True)
        datas=PNF_transaction(buy_data, sell_data)
        df_merge = pd.concat([df_merge, datas],ignore_index=True)

    print("df_merge is")
    df_merge.drop(["index","TranType","Active","Sell_Symbol"],axis=1,inplace=True)
    df_merge['Buy_Val']=df_merge['Buy_Qty'] * df_merge['Buy_Avg']
    df_merge['Sell_Val']=df_merge['Sell_Qty'] * df_merge['Sell_Avg']

    df1=pd.read_excel(loc,sheet_name='LTP_trade')
    df1=df1[['Stock','lastPrice']]
    df1.rename(columns={'Stock':'Symbol'},inplace=True)
    df_merge = pd.merge(df_merge,df1, on=['Symbol'],how='left')

    df_merge = df_merge.replace(np.nan,0)
    df_merge['Net_Position']=df_merge['Buy_Qty'] - df_merge['Sell_Qty']
    df_merge['Inventory_Value']=df_merge['Net_Position'] * df_merge['lastPrice']
    df_merge['Profit/Loss $'] = np.where(df_merge['Net_Position']!=0,(df_merge['Inventory_Value'] + df_merge['Sell_Val'] - df_merge['Buy_Val']),(df_merge['Sell_Val'] - df_merge['Buy_Val']))
    df_merge['Profit/Loss $']=round(df_merge['Profit/Loss $'],0)
    df_merge['Profit/ Loss %'] =round((df_merge['Profit/Loss $'] / df_merge['Buy_Val'])*100,2)
    print(df_merge)
    return df_merge


sheet1 = workbook.sheets['Net_Position'].used_range.value
Delivery_flow_Position = xw.sheets('LTP_trade')
df = pd.DataFrame(sheet1)

df_new = df[[1, 5]]
df_new = pd.DataFrame(df_new)
print(df_new)
df_new = (df_new.drop_duplicates(subset=[1, 5]))
print(df_new)
df_new = df_new[df_new[1].notnull()]
df_new = df_new[df_new[1] != 0]

nse_list = df_new[df_new[5] == 'NSE']
bse_list = df_new[df_new[5] == 'BSE']
fno_list = df_new[df_new[5] == 'FNO']
stock_nse = nse_list[1].tolist()
stock_bse = bse_list[[1]].values.tolist()
stock_fno = fno_list[1].tolist()

# call functions
if len(stock_nse) != 0:
    print('getting nse data')
    nse_details(stock_nse)
if len(stock_bse) != 0:
    print('getting bse data')
    bse_details(stock_bse)

bse_all_df = pd.DataFrame()
for item in del_all_bse:
    bse_all_df = pd.concat([bse_all_df, pd.DataFrame(item)])

delivery_position_all_df = pd.DataFrame()
for item in delivery_position_all_list:
    delivery_position_all_df = pd.concat([delivery_position_all_df, pd.DataFrame(item)])


try:

    if len(bse_all_df)>0 and len(delivery_position_all_df)>0:
        bse_all_df = bse_all_df[
            ["Stock", "secWiseDelPosDate", "quantityTraded", "deliveryQuantity", "deliveryToTradedQuantity",
             "lastPrice", "pChange", "vwap"]]
        delivery_position_all_df = delivery_position_all_df[
            ["Stock", "secWiseDelPosDate", "quantityTraded", "deliveryQuantity",
             "deliveryToTradedQuantity", "lastPrice", "pChange", "vwap"]]
        merge = pd.concat([delivery_position_all_df, bse_all_df])
    elif len(delivery_position_all_df)>0:
        delivery_position_all_df = delivery_position_all_df[
            ["Stock", "secWiseDelPosDate", "quantityTraded", "deliveryQuantity",
             "deliveryToTradedQuantity", "lastPrice", "pChange", "vwap"]]
        merge = delivery_position_all_df
    elif len(bse_all_df)>0:
        bse_all_df = bse_all_df[
            ["Stock", "secWiseDelPosDate", "quantityTraded", "deliveryQuantity", "deliveryToTradedQuantity",
             "lastPrice", "pChange", "vwap"]]
        merge = bse_all_df

    Delivery_flow_Position.range('A2:H1000').clear()
    Delivery_flow_Position.range("A1").options(index=None).value = merge
except:
    pass


workbook.save()
time.sleep(5)
print("calculating p n l")
PNF = Profit_And_Loss()

ws1 = workbook.sheets['Profit_Loss']
ws1.range('A2:N5000').clear()
PNF['count_of_buy_qty'] = 0
PNF = PNF[["Scrip_Name", "Symbol", "Exchange", "Buy_Qty", "Sell_Qty",
                         "Buy_Val", "Sell_Val", "count_of_buy_qty", "lastPrice", "Net_Position", "Inventory_Value",
           "Profit/Loss $", "Profit/ Loss %"]]
ws1.range("A2").options(index=None).value = PNF
workbook.save()
# app=xw.apps.active.api
# app.Quit()
#workbook.close()




