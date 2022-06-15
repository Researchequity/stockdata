import requests
from requests.exceptions import HTTPError
import pandas as pd
import numpy as np
import os
import time
from datetime import timedelta, date
import threading
from filepath import *

def Analysis(df,filename):
    try:
        avg_all = []
        threads = []

        df['Open'] = pd.to_numeric(df['Open'], errors='coerce')
        df['Close'] = pd.to_numeric(df['Close'], errors='coerce')
        df['High'] = pd.to_numeric(df['High'], errors='coerce')
        df['Low'] = pd.to_numeric(df['Low'], errors='coerce')

        def avg_thread(stock, df):
            try:
                data = df[df['Index_Name'] == stock]
                data['moving_avg_20_current'] = data['Close'].rolling(window=20).mean()
                data['Prev_10d_20dMA'] = round(data['moving_avg_20_current'].shift(10),2)

                data['Abv_prev_10d_Close'] = round(((data['Close'] - data['Close'].shift(10)) / data['Close']) * 100, 2)
                data['Abv_prev_20d_Close'] = round(((data['Close'] - data['Close'].shift(20)) / data['Close']) * 100, 2)
                data['Abv_prev_50d_Close'] = round(((data['Close'] - data['Close'].shift(50)) / data['Close']) * 100, 2)

                data['moving_avg_20_current'] = round(data['moving_avg_20_current'], 2)
                data['Close_abv_20MA'] = np.where(data['Close'] > data['moving_avg_20_current'], 1, 0)
                data['Body_perct'] = round(((abs(data['Open'] - data['Close']) / abs(data['High'] - data['Low'])) * 100), 2)
                data['Color'] = np.where(data['Close'] > data['Open'], 'Green', 'Red')
                data['Strength_ratio'] = round(data['moving_avg_20_current'] / data['moving_avg_20_current'].shift(10), 2)

                data['Strength'] = np.where((data['Close'] > data['moving_avg_20_current']) & (data['moving_avg_20_current'] > data['moving_avg_20_current'].shift(10)), 1, 0)
                data['Engulfing'] = np.where((data['High'] > data['High'].shift(1)) & (data['Low'] < data['Low'].shift(1)), 1, 0)
                avg_all.append(data)

            except:
                pass
        uniqueValues = df['Index_Name'].unique().tolist()
        for stock in uniqueValues:
            t1 = threading.Thread(target=avg_thread, args=(stock, df))
            t1.start()
            threads.append(t1)

        for process in threads:
            process.join()

        df_stock = pd.concat(avg_all)
        df_stock.to_csv(PROCESSED_DIR + '//' + str(filename),index=None)

    except:
        print("There is Something Wrong")

def Cleaning(filename):
    try:
        df=pd.read_csv(RAW_DIR +'\\'+ str(filename))
        df.drop(['Turnover (Rs. Cr.)', 'P/E','P/B','Div Yield'], axis='columns', inplace=True)

        Index_Name=pd.read_csv(METADATA_DIR +'\\'+ 'Index_Name.csv')
        Index_Name=Index_Name.iloc[:, 0].values
        for IN in Index_Name:
            df.drop(df.index[df['Index Name'] == str(IN)], inplace=True)
        df.rename(columns={'Index Name':'Index_Name','Index Date':'Date','Open Index Value':'Open','High Index Value':'High','Low Index Value':'Low','Closing Index Value':'Close','Points Change':'Points_Change','Change(%)':'Change_perct'},inplace=True)

        if not os.path.exists(PROCESSED_DIR + '\\' + 'Ind_In_Close_Historical.csv'):
            df.to_csv(PROCESSED_DIR + '\\' + 'Ind_In_Close_Historical.csv', index=False)
        else:
            Historical = pd.read_csv(PROCESSED_DIR + '\\' + 'Ind_In_Close_Historical.csv')

            Historical['Date'] = pd.to_datetime(Historical['Date'], format='%d-%m-%Y')
            Historical['Date'] = Historical['Date'].dt.strftime('%Y-%m-%d')
            df['Date'] = pd.to_datetime(df['Date'], format='%d-%m-%Y')
            df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')

            duplicate = [value for value in df['Date'].unique().tolist() if value in Historical['Date'].unique().tolist()]
            if bool(duplicate)==False:
                Historical = pd.concat([df, Historical])
                Historical.sort_values(by=['Date'], inplace=True, ascending=True)
                Historical['Date'] = pd.to_datetime(Historical['Date'], format='%Y-%m-%d')
                Historical['Date'] = Historical['Date'].dt.strftime('%d-%m-%Y')
                Analysis(Historical,'Ind_In_Close_Historical.csv')
            else:
                print("This Date Data are Already Exists")
    except:
        print("There is Something Wrong with Cleaning Data")

def NSECollectionOfDates():
    try:
        All_date = []
        def daterange(date1, date2):
            for n in range(int((date2 - date1).days) + 1):
                yield date1 + timedelta(n)
        weekdays = [5, 6]
        for dt1 in daterange(start_dt, end_dt):
            if dt1.weekday() not in weekdays:  # to print only the weekdates
                a = dt1.strftime("%d%m%Y")
                All_date.append(a)
        # TWD HOLIDAYS FILE
        NSEHolidays = pd.read_csv(METADATA_DIR + '\\' + 'NSEHolidays.csv')
        NSEHolidays = NSEHolidays.iloc[:, 0].values
        for h in NSEHolidays:
            h = h.replace("-", "")
            if str(h) in All_date:
                All_date.remove(str(h))
        return All_date
    except:
        print("There is No Data Available for this Dates")

def Daily_Snapshot_data():
    try:
        All_date = NSECollectionOfDates()
        for date1 in All_date:
            link='https://archives.nseindia.com/content/indices/ind_close_all_'+str(date1)+'.csv'
            try:
                time.sleep(5)
                response = requests.get(link, timeout=10)
                response.raise_for_status()

                filename = 'ind_close_all_' + str(date1) + '.csv'
                open(RAW_DIR + '\\' + str(filename), 'wb').write(response.content)
                Cleaning(filename)
            except HTTPError as http_err:
                print(f'HTTP error occurred: {http_err}')
            except Exception as err:
                print(f'Other error occurred: {err}')
    except:
        print("Something went wrong")

try:
    historical_data = 0
    if historical_data == 0:
        start_dt = end_dt = date.today()
        Daily_Snapshot_data()
    else:
        start_dt = date(2022,3,18)     # Starting Date in %y,%m,%d Format Not use Zero Padding
        end_dt = date(2022,3,21)       # Ending Date in %y,%-m,%-d Format Not use Zero Padding
        Daily_Snapshot_data()
except:
    print("This Date data is Not Avilable")
