# Create Pre open mean file of all previous day file
import pandas as pd
from glob import glob
from filepath import *
from datetime import datetime

def get_pre_open_average():
    pre_open_all_df =pd.DataFrame()
    for file in glob(aggregate_file_dir + 'preopen/preopen_Historical_*.csv'):
        print(file)
        pre_open_df = pd.read_csv(file)
        pre_open_all_df = pd.concat([pre_open_all_df,pre_open_df])

    pre_open_all_df = pre_open_all_df.groupby(['token']).agg({'count_T': ['mean'], 'sum_t': ['mean']})
    pre_open_all_df = pre_open_all_df.reset_index()
    pre_open_all_df.columns = ['token','trade_mean','qty_mean']
    pre_open_all_df.to_csv(aggregate_file_dir + "preopen/preopen_average.csv",index=False)

def get_preopen_daily(): #generate preopen_Historical_date.csv
    # Create Preopen File
    date = ''.join(str(datetime.today().date()).split('-'))

    # temp_date = pd.to_datetime(str(datetime.today().date()) +' 09:15:00')

    #date = '20210616'
    # file_dir = 'F:\\Archive_Clean\\20201211\\DUMP_'
    # temp_date = pd.to_datetime('2020-12-11 09:15:00')

    df1 = pd.read_csv(dumper_file_dir + 'DUMP_' + date + '_073001.csv_Clean', header = None, nrows = 500000)
    df2 = pd.read_csv(dumper_file_dir + 'DUMP_' + date + '_073002.csv_Clean', header = None, nrows = 500000)
    print('half done')
    df3 = pd.read_csv(dumper_file_dir + 'DUMP_' + date + '_073003.csv_Clean', header = None, nrows = 500000)
    df4 = pd.read_csv(dumper_file_dir + 'DUMP_' + date + '_073004.csv_Clean', header = None, nrows = 500000)

    last_min = pd.to_datetime(df1[7].max())

    TODAY_DAY= last_min.day
    TODAY_MONTH= last_min.month
    TODAY_YEAR= last_min.year

    temp_date = datetime(day=TODAY_DAY ,month=TODAY_MONTH ,year=TODAY_YEAR ,hour=9 ,minute=15 ,second=0)

    # token_df = pd.read_csv('C:\\Python_Ankit\\Token_security.csv')
    # token_mean_df = mean_df.merge(token_df, left_on='Symbol', right_on= 'Symbol', how='outer')

    print('done reading')

    # In[26]:
    df1[7] = pd.to_datetime(df1[7])
    df2[7] = pd.to_datetime(df2[7])
    df3[7] = pd.to_datetime(df3[7])
    df4[7] = pd.to_datetime(df4[7])

    df1 = df1[df1[7] < temp_date]
    df2 = df2[df2[7] < temp_date]
    df3 = df3[df3[7] < temp_date]
    df4 = df4[df4[7] < temp_date]


    df = pd.concat([df1, df2])
    df = pd.concat([df, df3])
    df = pd.concat([df, df4])

    df[7] = df[7].dt.date

    df = df[df[1 ]!= 'X']

    dummy_df = df[df[1]=='T']

    df = df[df[1]!= 'T']
    df.reset_index(drop = True, inplace=True)
    #######################################################

    temp = df.groupby([7, 3, 4]).agg({4 : ['count'], 6: ['sum']}).reset_index()
    dummy_df = dummy_df.groupby([7, 4]).agg({4 : ['count'], 6: ['sum'], 5: ['mean']}).reset_index()
    temp.columns = ['date', 'token', 'status', 'count', 'sum']
    dummy_df.columns = ['date', 'token', 'count', 'sum' ,'price']

    temp = temp.pivot(index = ['date', 'token'], columns=['status'], values=['count', 'sum']).reset_index()
    # temp.columns = ['date', 'token', 'count_Buy', 'count_Sell', 'sum_Buy', 'sum_Sell']
    temp.columns = list(map("_".join, temp.columns))
    temp.rename(columns = {"token_" : "token"}, inplace = True)
    dummy_df['token'] = dummy_df['token'].astype(int)
    final_df = pd.merge(temp, dummy_df, on = 'token', how = 'outer')

    # final_df = pd.concat([final_df, temp])
    final_df.columns = ['Date', 'token', 'count_Buy', 'count_Sell', 'sum_Buy', 'sum_Sell', 'date_', 'count_T', 'sum_t','price']
    final_df = final_df[['Date', 'token', 'count_Buy', 'count_Sell', 'sum_Buy', 'sum_Sell', 'count_T', 'sum_t' ,'price']]
    final_df.to_csv(aggregate_file_dir + '/preopen/preopen_Historical_' +date +'.csv', index=False)

    mean_df = pd.read_csv(aggregate_file_dir + '/preopen/preopen_average.csv')
    final_df = final_df.merge(mean_df, left_on='token', right_on= 'token', how='left')
    final_df['Qty_ratio'] = (final_df['sum_t' ] /final_df['qty_mean']).round(decimals=1)
    final_df['Trade_ratio'] = (final_df['count_T' ]//final_df['trade_mean']).round(decimals=1)
    final_df['Trade_value'] = final_df['price' ] *final_df['sum_t']

    final_df.to_csv(aggregate_file_dir + '/preopen/preopen_Historical_' +date +'.csv', index=False)

if __name__ == '__main__':

    try:
        get_pre_open_average()
    except:
        pass

    try:
        get_preopen_daily()
    except:
        pass

