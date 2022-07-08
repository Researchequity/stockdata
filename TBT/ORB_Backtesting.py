import time
import pandas as pd
import glob
import os
import datetime
import re
import dataframe_image as dfi

import threading
import numpy as np
import warnings

warnings.simplefilter(action="ignore", category=Warning)

Filepath=r'\\192.168.41.190\program\stockdata\ORB'

threads=[]
df_all=[]


today_date=datetime.datetime.today().strftime("%Y%m%d")
#today_date="20220607"
filename='ohlc_'+today_date+'.csv'
print(filename)

ohlc = pd.read_csv(Filepath+'\\'+filename)
ohlc.columns = ['Date', 'script_code', 'open', 'high', 'low', 'close', 'price_traded', 'volume']
##ohlc['Date'] = pd.to_datetime(ohlc['Date'], format='%d-%m-%Y %H:%M')
#ohlc['Date'] = ohlc['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')

ohlc['open'] = round(ohlc['open'] * 0.01, 2)
ohlc['high'] = round(ohlc['high'] * 0.01, 2)
ohlc['low'] = round(ohlc['low'] * 0.01, 2)
ohlc['close'] = round(ohlc['close'] * 0.01, 2)

r=re.sub('\D','',filename)
date=datetime.datetime.strptime(r,"%Y%m%d")
date = date.strftime('%Y-%m-%d')
end_candle_time_15m = date + ' ' + datetime.time(9, 30, 0).strftime('%H:%M:%S')

def avg_thread(sc):
    SC_Data = ohlc[ohlc['script_code'] == sc]
    merge_sc = SC_Data.loc[SC_Data['Date'] <= end_candle_time_15m]

    if(len(merge_sc['Date']) > 13):
        merge_sc['15_Min_High']=merge_sc['high'].max()

        merge_sc['15_Min_Low']=merge_sc['low'].min()
        merge_sc['15_Min_Volume']=sum(merge_sc['volume'])
        merge_sc['Date'] = pd.to_datetime(merge_sc['Date'], format='%Y-%m-%d %H:%M:%S')
        merge_sc['Date'] = merge_sc['Date'].dt.strftime('%Y-%m-%d')
        merge_sc = merge_sc.groupby(['Date', 'script_code', '15_Min_Volume']).max()
        merge_sc = merge_sc.reset_index().rename_axis(None, axis=1)
        merge_sc=merge_sc[['Date','script_code','15_Min_Volume','15_Min_High','15_Min_Low']]
        df_all.append(merge_sc)
    else:
        df_all.append(pd.DataFrame())
        pass

sript_code_list = ohlc['script_code'].unique().tolist()
for sc in sript_code_list:
    t1 = threading.Thread(target=avg_thread, args=(sc,))
    t1.start()
    threads.append(t1)

    for process in threads:
        process.join()

    df_stock = pd.concat(df_all)

print(df_stock)

Historical = pd.read_csv(Filepath+'\\'+'ORB_Backtesting.csv')
Historical=Historical[['Date','script_code','15_Min_Volume','15_Min_High','15_Min_Low']]

duplicate = [value for value in df_stock['Date'].unique().tolist() if value in Historical['Date'].unique().tolist()]
if bool(duplicate) == False:
    grp = pd.concat([Historical, df_stock])

    df1 = pd.DataFrame()

    sript_code_list = grp['script_code'].unique().tolist()
    for sc in sript_code_list:
        merge_sc = grp[grp['script_code'] == sc]
        merge_sc['moving_avg_10_current'] = merge_sc['15_Min_Volume'].rolling(window=10).mean()
        df1 = pd.concat([df1, merge_sc])

    stockmetadata = pd.read_csv(r'\\192.168.41.190\program\stockdata\metadata\StockMetadata.csv')
    stockmetadata = stockmetadata[['token', 'Symbol', 'MarketCap']]
    stockmetadata.rename(columns={'token': 'script_code'}, inplace=True)
    df1 = pd.merge(df1, stockmetadata, on=['script_code'], how='left')
    df1['ratio'] = df1['15_Min_Volume'] / df1['moving_avg_10_current']

    df1.to_csv(Filepath+'\\'+"ORB_Backtesting.csv",index=False)
    print(date)
    df2 = df1[df1['Date'] == date]
    df2 = df2[df2['MarketCap'] == 'Mid']
    df2 = df2[df2['ratio'] > 4.0]

    df1 = df1[df1['Date'] == date]
    df1 = df1[df1['MarketCap'] == 'LARGE']
    df1 = df1[df1['ratio'] >2.0]


    df1=pd.concat([df1,df2])
    print(df1)
    #RTP = int(input("Enter Value Risk Per Trade"))
    RTP = 1000
    MID_RTP = RTP * 5
    df1['Candle_Body_Pert']=round(((df1['15_Min_High']-df1['15_Min_Low'])/df1['15_Min_Low'])*100)
    df1['RPT']=np.where(df1['MarketCap'] == 'LARGE',RTP,MID_RTP)
    df1['Price_Diff']=df1['15_Min_High']-df1['15_Min_Low']

    #df1['Qty']=round(df1['RPT']/df1['Price_Diff'])

    df1['Trigger_Buy']=round(0.05 * round((df1['15_Min_High']*1.001) / 0.05), 2)#df1['15_Min_High']*1.005
    df1['Trigger_Sell']=round(0.05 * round((df1['15_Min_Low']*0.999) / 0.05), 2)#df1['15_Min_Low']*0.995

    df1['Buy']=round(0.05 * round((df1['Trigger_Buy']*1.005) / 0.05), 2)#df1['15_Min_High']*1.005
    df1['Sell']=round(0.05 * round((df1['Trigger_Sell']*0.995) / 0.05), 2)#df1['15_Min_Low']*0.995

    df1['Qty'] = round(df1['RPT'] / (df1['Trigger_Buy']-df1['Trigger_Sell']))

    df1['Order_Type']='SL'
    df1=df1[df1['Candle_Body_Pert'] < 5]
    df1.to_csv(Filepath+'\\'+"todays_stocks.csv", index=False)
    df1.to_csv(Filepath+'\\'+"Historical_todays_stocks.csv",mode='a',header=False, index=False)
    dfi.export(df1, Filepath+'\\'+"Todays_Stock.png")
    os.remove(Filepath+'\\'+filename)


else:
    print("This Date Data are Already Exists")











"""
    #Tokens code
    symbol_list_arr = []

    exchange = 'NSE'
    instruments = pd.read_csv('instruments.csv')
    NSE = instruments[instruments['exchange'].str.match(exchange)]

    symbol = df1['Symbol'].unique().tolist()

    for token in symbol:
        symbol = NSE[NSE['tradingsymbol'] == token]
        tradingsymbol = symbol['tradingsymbol'].to_string(index=False)
        instrument_token = symbol['instrument_token'].to_string(index=False)
        symbol_list = [instrument_token,tradingsymbol]
        symbol_list_arr.append(symbol_list)

    df = pd.DataFrame(symbol_list_arr, columns=['token_id','token_name'])
    print(df)
    df.to_csv("tokens.csv", index=False)


"""