import pandas as pd
import numpy as np
from utils import *

workingarea = r'\\192.168.41.190\program\stockdata\processed\del_data_NSE'

date_today = datetime.date.today()  # - datetime.timedelta(days=3)
date_today, prev_date, fut_date = get_market_prev_date_fut_date(date_today)
dat = date_today.strftime('%Y%m%d')
temp_date = date_today.strftime('%d-%m-%Y')
file = os.path.basename(__file__)

# get historical strength and average
nse_del_hist = pd.DataFrame()
from glob import glob

for file in glob(workingarea + '\\nse_delivery_position_*.csv'):
    print(file)
    Working_df = pd.read_csv(file)[['Stock', 'secWiseDelPosDate', 'lastPrice', 'pChange', 'vwap', 'quantityTraded',
                                    'deliveryQuantity', 'deliveryToTradedQuantity', 'Series']]
    Working_df['secWiseDelPosDate'] = pd.to_datetime(Working_df['secWiseDelPosDate'], format='%d-%m-%Y %H:%M')
    nse_del_hist = pd.concat([nse_del_hist, Working_df])


nse_del_hist['trade_value'] = nse_del_hist['lastPrice'] * nse_del_hist['quantityTraded']
nse_del_hist = nse_del_hist.groupby(['secWiseDelPosDate']).agg({'Stock': ['count'], 'trade_value': ['sum']}).reset_index()
nse_del_hist.columns = ['secWiseDelPosDate','Stock_count','trade_value_sum']
nse_del_hist = nse_del_hist[(nse_del_hist['Stock_count'] >= 1600) & (nse_del_hist['Stock_count'] <= 2000)]
nse_del_hist['secWiseDelPosDate'] = pd.to_datetime(nse_del_hist['secWiseDelPosDate'], format='%d-%m-%Y %H:%M')

nse_del_hist['temp_dat'] = nse_del_hist['secWiseDelPosDate'].dt.strftime('%d-%m-%Y')
nse_del_hist['temp_time'] = nse_del_hist['secWiseDelPosDate'].dt.strftime('%H')
nse_del_hist['secWiseDelPosDate'] = nse_del_hist['secWiseDelPosDate'].dt.strftime('%d-%m-%Y')

# call average
nse_del_hist['avg_stock'] = nse_del_hist['temp_time']
nse_del_hist['avg_date'] = nse_del_hist['temp_dat']
nse_del_hist['avg_col'] = nse_del_hist['trade_value_sum']

nse_del_hist.sort_values(by=['temp_dat'], inplace=True, ascending=False)  #
numpy_date = np.datetime64(date_today)
df_stock_avg_all = average(nse_del_hist, numpy_date)
df_stock_avg_all = df_stock_avg_all.rename(columns={"avg_norm_mean": "strength_historical"})
df_stock_avg_all.to_csv(RAW_DIR + '\\nse_del_hist_avg.csv')

# get today market data strength
nse_del_today = pd.read_csv(PROCESSED_DIR + '\\nse_delivery_position.csv')[
    ['Stock', 'secWiseDelPosDate', 'quantityTraded', 'lastPrice', 'pChange', 'vwap']]
# temp_df = nse_del_today['pChange'].tolist()

nse_del_today['strength_today'] = nse_del_today['lastPrice'] * nse_del_today['quantityTraded']
nse_del_today['secWiseDelPosDate'] = pd.to_datetime(nse_del_today['secWiseDelPosDate'], format='%d-%m-%Y %H:%M')
nse_del_today['temp_dat'] = nse_del_today['secWiseDelPosDate'].dt.strftime('%d-%m-%Y')
nse_del_today['temp_time'] = nse_del_today['secWiseDelPosDate'].dt.strftime('%H')

# sum of market strength and compare with temp time
nse_del_today['avg_stock'] = nse_del_today['temp_time']
nse_del_today['avg_date'] = nse_del_today['temp_dat']
nse_del_today['avg_col'] = nse_del_today['lastPrice']
nse_temp = nse_del_today
nse_del_today.sort_values(by=['temp_dat'], inplace=True, ascending=True)
nse_del_today = nse_del_today.groupby(['temp_time', 'temp_dat']).agg({'strength_today': ['sum'], 'Stock': ['count']}).reset_index()
nse_del_today.columns = ['temp_time', 'temp_dat', 'strength_today_sum', 'stock_count_today']

df_merge = pd.merge(nse_del_today, df_stock_avg_all, on=['temp_time'], how='left')
df_merge = df_merge[['strength_today_sum', 'stock_count_today', 'temp_time', 'strength_historical']]
df_merge = df_merge[(df_merge['stock_count_today'] >= 1600) & (df_merge['stock_count_today'] <= 2000)]
df_merge['perct'] = df_merge['strength_today_sum'] / df_merge['strength_historical']


# check pchange count hourly
time_list = nse_temp['temp_time'].unique()
pchng_list = []
for t in time_list:
    temp = nse_temp[nse_temp['temp_time'] == t]
    pos_count, neg_count = 0, 0
    for num in temp['pChange']:
        if num > 0:
            pos_count += 1
        elif num < 0:
            neg_count += 1
    pchng_list.append(pos_count)
    pchng_list.append(neg_count)
    pchng_list.append(t)

pchng_df = pd.DataFrame(pchng_list)
pchng_df = pd.DataFrame(pchng_df.values.reshape(-1, 3), columns=['pos_stock', 'neg_stock', 'time'])
pchng_df.sort_values(by=['time'],ascending=False, inplace=True) # desc

pchng_df.to_csv(RAW_DIR + '\\mrkt_pchng_count.csv', index=None)
df_merge.to_csv(RAW_DIR + '\\mrkt_strength.csv', index=None)
