import pandas as pd
from collections import Counter
from datetime import datetime
import getpass
import numpy as np

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)


#Change date for file read  and comment date from today
date = '20210111'
date = ''.join(str(datetime.today().date()).split('-'))
print(date)

read_row = 0
while 0 == 0:
    try:
        num_trades = pd.read_csv('//home/workspace/aggregate/trade_result_final_' +date + '.csv',header=None,skiprows=read_row,nrows=10)
        read_row=read_row+len(num_trades)
        num_trades.columns = ['Date','Token','Stock','row_count','sum_qty','buy','sell','vwap','norm_mean','morm_trd','nbuy_qty','nsell_qty']

        
        num_trades['Qty_ratio'] =num_trades['sum_qty']//num_trades['norm_mean']

        print(num_trades)
    except:
        continue
    

  
