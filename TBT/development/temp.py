import pandas as pd
import glob
import os
import datetime
import re
import warnings
import threading

warnings.simplefilter(action="ignore", category=Warning)

pd.set_option('display.max_columns', None)

filepath=r"/home/workspace/aggregate/"
filelist=sorted(glob.glob(filepath+'//'+'ohlc_*.csv'))
#a=sorted(filelist)
#print(a)

#filelist=['ohlc_20220103.csv']
df = pd.DataFrame()
threads=[]
df_all=[]

for path in filelist:
    print("path is ",path)
    path=os.path.basename(path)
    ohlc = pd.read_csv(filepath+'//'+path)
    ohlc.columns = ['Date', 'script_code', 'open', 'high', 'low', 'close', 'price_traded', 'volume']

    ohlc['open'] = round(ohlc['open'] * 0.01, 2)
    ohlc['high'] = round(ohlc['high'] * 0.01, 2)
    ohlc['low'] = round(ohlc['low'] * 0.01, 2)
    ohlc['close'] = round(ohlc['close'] * 0.01, 2)
    r=re.sub('\D','',path)
    date=datetime.datetime.strptime(r,"%Y%m%d")
    date = date.strftime('%Y-%m-%d')
    start_candle_time_15m = date + ' ' + datetime.time(9, 16, 0).strftime('%H:%M:%S')
    end_candle_time_15m = date + ' ' + datetime.time(9, 30, 0).strftime('%H:%M:%S')
    end_candle_time_310 = date + ' ' + datetime.time(15,10, 0).strftime('%H:%M:%S')

    #df = pd.DataFrame()


    #ohlc.drop_duplicates(subset=ohlc.columns.tolist(),inplace=True)# keep=False)#, inplace=True)
    def avg_thread(sc):
        try:
            SC_Data = ohlc[ohlc['script_code'] == sc]
            After15min=SC_Data.loc[(end_candle_time_15m < SC_Data['Date']) & (SC_Data['Date'] <= end_candle_time_310)]
            merge_sc = SC_Data.loc[(start_candle_time_15m <= SC_Data['Date']) & (SC_Data['Date'] <= end_candle_time_15m)]

            if(len(merge_sc['Date']) > 13):
                merge_sc['15_Min_High']=merge_sc['high'].max()
                merge_sc['15_Min_Low']=merge_sc['low'].min()
                merge_sc['Day_High_Until_3:10'] = After15min['high'].max()
                merge_sc['Day_Low_Until_3:10']=After15min['low'].min()
                merge_sc['15_Min_Volume']=sum(merge_sc['volume'])
                merge_sc['Close_Price_3:10']=After15min['close'].tail(1).values[0]
                merge_sc['Close_Price_DateTime']=After15min['Date'].tail(1).values[0]

                df_all.append(merge_sc)

            else:
                pass
        except:
            pass
    sript_code_list = ohlc['script_code'].unique().tolist()
    for sc in sript_code_list:
        t1 = threading.Thread(target=avg_thread, args=(sc,))
        t1.start()
        threads.append(t1)

    for process in threads:
        process.join()

    df_stock = pd.concat(df_all)
    df=pd.concat([df,df_stock])

df1 = pd.DataFrame()

mohlc=df

mohlc['Date'] = pd.to_datetime(mohlc['Date'], format='%Y-%m-%d %H:%M:%S')
mohlc['Date'] = mohlc['Date'].dt.strftime('%Y-%m-%d')
grp=mohlc.groupby(['Date','script_code','15_Min_Volume']).max()
grp = grp.reset_index().rename_axis(None, axis=1)
sript_code_list = grp['script_code'].unique().tolist()
for sc in sript_code_list:
    merge_sc = grp[grp['script_code'] == sc]
    merge_sc['moving_avg_10_current'] = merge_sc['15_Min_Volume'].rolling(window=10).mean()
    df1 = pd.concat([df1, merge_sc])

stockmetadata = pd.read_csv(r'D:\Program' + '\\' + 'StockMetadata.csv')
stockmetadata=stockmetadata[['token','Symbol','MarketCap']]

stockmetadata.rename(columns={'token': 'script_code'}, inplace=True)
df1 = pd.merge(df1, stockmetadata, on=['script_code'], how='left')
#'15_Min_High','15_Min_Low'
df1['(i-j)*100/j']=((df1['15_Min_High']-df1['15_Min_Low'])*100)/df1['15_Min_Low']
df1['ratio']=df1['15_Min_Volume']/df1['moving_avg_10_current']
df1=df1[['Date','script_code','Symbol','15_Min_Volume','moving_avg_10_current','MarketCap','15_Min_High','15_Min_Low','Day_High_Until_3:10','Day_Low_Until_3:10','Close_Price_3:10','(i-j)*100/j','ratio','Close_Price_3:10','Close_Price_DateTime']]
print(df1)
df1.to_csv(filepath+'//'+ 'FINAL_OHLC_DATA.csv', index=False)









































    #df_stock.to_csv(, index=None)

    #df1 = pd.DataFrame()
"""
#filelist=['ohlc_20220506.csv']
for path in filelist:
    t1 = threading.Thread(target=cleaning, args=(path,))
    t1.start()
    threads.append(t1)

for process in threads:
    process.join()

df = pd.concat(df_all)
"""



















"""
sript_code_list = mohlc['script_code'].unique().tolist()

for sc in sript_code_list:
    merge_sc = mohlc[mohlc['script_code'] == sc]
    merge_sc['moving_avg_15_current'] = merge_sc['volume'].rolling(window=150).mean()
    df = pd.concat([df, merge_sc])

df.to_csv(r'D:\Program' + '\\' + 'ttt.csv', index=False)
#a=mohlc['volume'].shift(10)


if not os.path.exists(filepath + '\\' + 'MergeOHLC.csv'):
    new_ohlc.to_csv(filepath + '\\' + 'MergeOHLC.csv', index=False)
else:
    new_ohlc.to_csv(filepath + '\\' + 'MergeOHLC.csv',mode='a', index=False,header=False)

#print(filelist)
"""
