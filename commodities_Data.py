import pandas as pd
from tvDatafeed import TvDatafeed, Interval
from Historical_Daily_Snapshot import Analysis
import os
import datetime as dt
from filepath import *
import warnings

warnings.simplefilter(action="ignore", category=Warning)


tv = TvDatafeed()
username = 'kinjalchauhan1999@gmail.com'
password = 'kinjal12799'

#tv = TvDatafeed(auto_login=False)
#tv = TvDatafeed(username, password, chromedriver_path="C:\Program Files\Google\Chrome\Application\chrome.exe")

COPPER_data = tv.get_hist(symbol='COPPER',exchange='FX',interval=Interval.in_daily,n_bars=1)
COPPER_data['symbol']='Copper'

crudeoil_data = tv.get_hist(symbol='USOIL',exchange='FX',interval=Interval.in_daily,n_bars=1)
crudeoil_data['symbol']='Crude Oil WTI'

NATURALGAS_data = tv.get_hist(symbol='NATURALGAS',exchange='CAPITALCOM',interval=Interval.in_daily,n_bars=1)
NATURALGAS_data['symbol']='Natural Gas'

GOLD_data = tv.get_hist(symbol='GOLD',exchange='CAPITALCOM',interval=Interval.in_daily,n_bars=1)
GOLD_data['symbol']='Gold'

SILVER_data = tv.get_hist(symbol='SILVER',exchange='CAPITALCOM',interval=Interval.in_daily,n_bars=1)
SILVER_data['symbol']='Silver'

ALUMINUM_data = tv.get_hist(symbol='ALUMINUM',exchange='CAPITALCOM',interval=Interval.in_daily,n_bars=1)
ALUMINUM_data['symbol']='Aluminium'

df=pd.concat([COPPER_data,crudeoil_data,NATURALGAS_data,GOLD_data,SILVER_data,ALUMINUM_data])
print(df.columns)
df = df.reset_index().rename_axis(None, axis=1)

df.rename({'datetime':'Date','symbol':'Index_Name','close':'Close','open':'Open','high':'High','low':'Low','volume':'Volume'},inplace=True,axis=1)
#for i in range(0,len(df)-1):
#    print(df.loc[i])
#    df=df.loc[i]
if not os.path.exists(PROCESSED_DIR + '\\' + 'Historical_Commodities_Data.csv'):
    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d %H:%M:%S')
    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
    df.sort_values(by=['Date'], inplace=True, ascending=True)
    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
    df['Date'] = df['Date'].dt.strftime('%d-%m-%Y')
    Analysis(df, 'Historical_Commodities_Data.csv')


else:
    Historical = pd.read_csv(PROCESSED_DIR + '\\' + 'Historical_Commodities_Data.csv')

    Historical['Date'] = pd.to_datetime(Historical['Date'], format='%d-%m-%Y')
    Historical['Date'] = Historical['Date'].dt.strftime('%Y-%m-%d')
    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d %H:%M:%S')
    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')

    duplicate = [value for value in list(zip(df['Date'], df['Index_Name'])) if
                 value in list(zip(Historical['Date'], Historical['Index_Name']))]
    if bool(duplicate) == False:
        Historical = pd.concat([df, Historical])
        Historical.sort_values(by=['Date'], inplace=True, ascending=True)
        Historical['Date'] = pd.to_datetime(Historical['Date'], format='%Y-%m-%d')
        Historical['Date'] = Historical['Date'].dt.strftime('%d-%m-%Y')

        Analysis(Historical, 'Historical_Commodities_Data.csv')
    else:
        print("This Date Data are Already Exists")
print(df)
