
# Create, get open price File

from datetime import datetime
import pandas as pd
import time
import os
from get_today_Open_Price_filepath import *



date = ''.join(str(datetime.today().date()).split('-'))
#date = '20210422'
#dumper_file_dir = 'Y:\\dumper\\'
#OPENING_DATA = 'E:\\Dumper\\'


OPENING_DATA_FILE=os.path.join(OPENING_DATA_FUT,"openingdata_{}.csv".format(date))

onetime = 0 

while datetime.now() <= datetime(year=datetime.today().year, day=datetime.today().day, month=datetime.today().month, hour=9, minute=25, second=00) or onetime == 0:

    if onetime == 1:
        time.sleep(180)

    onetime = 1  # print('done reading')

    df1 = pd.read_csv(dumper_file_dir + 'fno_' + date + '_073001.csv_Clean', header = None, nrows = 3000000)
    df2 = pd.read_csv(dumper_file_dir + 'fno_' + date + '_073002.csv_Clean', header = None, nrows = 3000000)
    print('half done')
    df3 = pd.read_csv(dumper_file_dir + 'fno_' + date + '_073003.csv_Clean', header = None, nrows = 1000000)
    df4 = pd.read_csv(dumper_file_dir + 'fno_' + date + '_073004.csv_Clean', header = None, nrows = 1000000)

    last_min = pd.to_datetime(df1[7].max())
    TODAY_DAY= last_min.day
    TODAY_MONTH= last_min.month
    TODAY_YEAR= last_min.year

    temp_date = datetime(day=TODAY_DAY,month=TODAY_MONTH,year=TODAY_YEAR,hour=9,minute=20,second=0)


    df1[7] = pd.to_datetime(df1[7])
    df1[8] = 1
    df2[7] = pd.to_datetime(df2[7])
    df2[8] = 2
    df3[7] = pd.to_datetime(df3[7])
    df3[8] = 3
    df4[7] = pd.to_datetime(df4[7])
    df4[8] = 4

    df1 = df1[df1[7] <= temp_date]
    df2 = df2[df2[7] <= temp_date]
    df3 = df3[df3[7] <= temp_date]
    df4 = df4[df4[7] <= temp_date]


    df = pd.concat([df1, df2])
    df = pd.concat([df, df3])
    df = pd.concat([df, df4])

    df[7] = df[7].dt.date   
    df = df[df[1]=='T']
    dummy_df = df.groupby([4,8]).agg({0 : ['min']}).reset_index()
    dummy_df.columns = [4,8,0] #dummy_df.columns = ['token','stream', 'unique_row']

    open_df = df.merge(dummy_df, on=[8,0,4] , how='inner')

    open_df = open_df[[4,5]]
    open_df.columns = ['token','open_price']


    open_df.to_csv(OPENING_DATA_FILE, header = None, index=False)





