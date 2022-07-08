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
        num_trades = pd.read_csv('//home/workspace/aggregate/stream_op_' +date + '.csv',header=None,skiprows=read_row,nrows=10)
        read_row=read_row+len(num_trades)
        num_trades.columns = ['Date','token','normean_quantity','sum_qty','normean_trd','row_count','vwap_price', 'o_price','BUY','repeat','r_qty', 'Symbol']
        num_trades['Date'] = num_trades['Date'].str[10:16]
        print(num_trades.to_string(index=False))
    except:
        continue
    

  
