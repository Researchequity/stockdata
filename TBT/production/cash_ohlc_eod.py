import pandas as pd
from datetime import datetime

path= '/home/workspace/aggregate/'

#Change date for file read  and comment date from today 
date = '20210409'
date = ''.join(str(datetime.today().date()).split('-'))

# read min ohlc data
df_ohlc_min = pd.read_csv(path +'ohlc_' + date + '.csv', header = None)
df_ohlc_min.columns = ['date','token','open','high','low','close','value','volume']
df_ohlc_min['date'] = pd.to_datetime(df_ohlc_min['date'])

#group by min data
df_ohlc_min.sort_values(by = ['token','date'], ascending = False)
df_ohlc_eod = df_ohlc_min.groupby(['token']).agg({'open':['first'],'high':['max'],'low':['min'],'value':['sum'],'volume':['sum']}).reset_index()
df_ohlc_eod.columns = ['token','open','high','low','value','volume']

#get vwap for last 30 min to get close price
last_min = pd.to_datetime(df_ohlc_min['date'].max())
TODAY_DAY= last_min.day
TODAY_MONTH= last_min.month
TODAY_YEAR= last_min.year
close_start_time = datetime(day=TODAY_DAY,month=TODAY_MONTH,year=TODAY_YEAR,hour=15,minute=1,second=00)
close_end_time = datetime(day=TODAY_DAY,month=TODAY_MONTH,year=TODAY_YEAR,hour=15,minute=30,second=00)

df_ohlc_min = df_ohlc_min[(df_ohlc_min['date']>=close_start_time) & (df_ohlc_min['date']<=close_end_time)]
df_vwap = df_ohlc_min.groupby(['token']).agg({'value':['sum'],'volume':['sum']}).reset_index()
df_vwap.columns = ['token','value_last30min','volume_last30min']
df_vwap['close'] = df_vwap['value_last30min']//df_vwap['volume_last30min']

df_ohlc_eod = pd.merge(df_ohlc_eod,df_vwap, on='token', how='left')
df_ohlc_eod.drop(['value_last30min','volume_last30min'], axis='columns', inplace=True)
df_ohlc_eod[['token','open','high','low','close','value','volume']].to_csv(path +'ohlcdaily'+date+'.csv',index=False)

#Get R3
df_ohlc_eod['pp'] = (df_ohlc_eod['high'] +df_ohlc_eod['low'] + df_ohlc_eod['close'])//3
df_ohlc_eod['R3'] = df_ohlc_eod['high'] + 2 *(df_ohlc_eod['pp'] - df_ohlc_eod['low'])
df_ohlc_eod[['token','open','high','low','close','value','volume','R3']].to_csv(path +'ohlcdaily.csv',index=False)


