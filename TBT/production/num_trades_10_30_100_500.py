import pandas as pd
from collections import Counter
from datetime import datetime
import getpass
import numpy as np

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)

python_ankit_dir = "/home/workspace/development/"


#Change date for file read  and comment date from today
date = '20210111'
date = ''.join(str(datetime.today().date()).split('-'))
aggregate_dir = '/home/workspace/aggregate/trade_result_final_' +date + '.csv'

print(aggregate_dir)
read_row = 0
while 0 == 0:
    try:
        num_trades = pd.read_csv(aggregate_dir,header=None,skiprows=read_row,nrows=10)
        read_row=read_row+len(num_trades)
        num_trades.columns = ['Date','Token','Stock','row_count','sum_qty','buy','sell','vwap','norm_mean','morm_trd','nbuy_qty','nsell_qty']
        num_trades['Qty_ratio'] =num_trades['sum_qty']//num_trades['norm_mean']

        #num_trades = num_trades[(num_trades[9]>10) & ((num_trades[4]/num_trades[8])>100)]
        
        
        stockmetadata_df = pd.read_csv(python_ankit_dir  +'python_ankit/StockMetadata.csv')
        stockmetadata_df = stockmetadata_df[['Symbol','MarketCap']]
        num_trades = num_trades.merge(stockmetadata_df, left_on='Stock',right_on = 'Symbol', how='left')
        num_trades.drop(['Symbol'], axis=1, inplace =True)
        
        cond = ((num_trades['Qty_ratio']>10) & (num_trades['MarketCap']=='LARGE')
            | (num_trades['Qty_ratio']>100) & (num_trades['MarketCap']=='Small')
            | (num_trades['Qty_ratio']>30) & (num_trades['MarketCap']=='Mid')
            | (num_trades['Qty_ratio']>500) & (num_trades['MarketCap']=='VSM'))
        num_trades = num_trades[cond]

        #Input from Excel
        if len(num_trades):
            print(num_trades)        
    except:
        continue


  
