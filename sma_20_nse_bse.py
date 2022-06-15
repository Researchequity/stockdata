import pandas as pd
from utils import *
date_today = datetime.date.today() #- datetime.timedelta(days=10)
date_today = np.datetime64(date_today)
PROCESSED_DIR = r'\\192.168.41.190\program\stockdata\processed'
threads = []
avg_all = []
avg_nse_all = []

# read files
bse_hist = pd.read_csv(PROCESSED_DIR + "\\bhav_data_bse_historical.csv")[['SC_CODE', 'TRADING_DATE', 'NO_OF_SHRS', 'CLOSE']]
nse_hist = pd.read_csv(PROCESSED_DIR + "\\bhav_data_nse_historical.csv")[['Stock', 'secWiseDelPosDate', 'quantityTraded', 'lastPrice']]
nse_hist['secWiseDelPosDate'] = pd.to_datetime(nse_hist['secWiseDelPosDate'], format='%d-%m-%Y %H:%M')
bse_hist['TRADING_DATE'] = pd.to_datetime(bse_hist['TRADING_DATE'], format='%d-%m-%Y')

# filter and sort
filt_date_nse = nse_hist['secWiseDelPosDate'].max() - datetime.timedelta(days=90)
filt_date_bse = bse_hist['TRADING_DATE'].max() - datetime.timedelta(days=90)
nse_hist = nse_hist[nse_hist['secWiseDelPosDate'] >= filt_date_nse]
bse_hist = bse_hist[bse_hist['TRADING_DATE'] >= filt_date_bse]
nse_hist.sort_values(by=['secWiseDelPosDate'], ascending=True, inplace=True)  # ascending=False
bse_hist.sort_values(by=['TRADING_DATE'], ascending=True, inplace=True)  # ascending=False


def avg_bse_thread(stock, bse_hist):
    try:
        bse_hist = bse_hist[bse_hist['SC_CODE'] == stock]
        bse_hist['20_ma'] = bse_hist['CLOSE'].rolling(window=20).mean()
        bse_hist['20_ma_vol'] = bse_hist['NO_OF_SHRS'].rolling(window=20).mean()
        bse_hist = bse_hist.rename(columns={"SC_CODE": "Stock", "TRADING_DATE": "secWiseDelPosDate", "CLOSE": "lastPrice", "NO_OF_SHRS": "quantityTraded"})
        bse_hist['20_ma_prev_30'] = bse_hist['20_ma'].iloc[-30]
        bse_hist['20_ma_vol_prev_30'] = bse_hist['20_ma_vol'].iloc[-30]
        avg_all.append(bse_hist)
    except:
        print('err')
        pass


def avg_nse_thread(stock, nse_hist):
    try:
        nse_hist = nse_hist[nse_hist['Stock'] == stock]
        nse_hist['20_ma'] = nse_hist['lastPrice'].rolling(window=20).mean()
        nse_hist['20_ma_vol'] = nse_hist['quantityTraded'].rolling(window=20).mean()
        nse_hist['20_ma_prev_30'] = nse_hist['20_ma'].iloc[-30]
        nse_hist['20_ma_vol_prev_30'] = nse_hist['20_ma_vol'].iloc[-30]
        avg_nse_all.append(nse_hist)
    except:
        pass


uniqueValues = bse_hist['SC_CODE'].unique().tolist()
for stock in uniqueValues:
    ic(stock)
    t1 = threading.Thread(target=avg_bse_thread, args=(stock, bse_hist))
    t1.start()
    threads.append(t1)

for process in threads:
    process.join()
df_stock = pd.concat(avg_all)
df_stock = df_stock[df_stock['20_ma_prev_30'].notna()]
df_stock = df_stock[df_stock['20_ma'].notna()]
df_stock = df_stock[df_stock['20_ma_vol'].notna()]
df_stock = df_stock[df_stock['20_ma_vol_prev_30'].notna()]
df_stock = df_stock[df_stock['secWiseDelPosDate'] == df_stock['secWiseDelPosDate'].max()]
# df_stock = df_stock[df_stock['secWiseDelPosDate'] >= (df_stock['secWiseDelPosDate'].max() - datetime.timedelta(days=20))]

uniqueValues = nse_hist['Stock'].unique().tolist()
for stock in uniqueValues:
    ic(stock)
    t1 = threading.Thread(target=avg_nse_thread, args=(stock, nse_hist))
    t1.start()
    threads.append(t1)

for process in threads:
    process.join()
df_nse = pd.concat(avg_nse_all)
df_nse = df_nse[df_nse['20_ma_prev_30'].notna()]
df_nse = df_nse[df_nse['20_ma'].notna()]
df_nse = df_nse[df_nse['20_ma_vol'].notna()]
df_nse = df_nse[df_nse['20_ma_vol_prev_30'].notna()]
df_nse = df_nse[df_nse['secWiseDelPosDate'] == df_nse['secWiseDelPosDate'].max()]
# df_nse = df_nse[df_nse['secWiseDelPosDate'] >= (df_nse['secWiseDelPosDate'].max() - datetime.timedelta(days=20))]

sma_20_nse_bse = pd.concat([df_stock, df_nse])
sma_20_nse_bse['ratio_20ma'] = (sma_20_nse_bse['20_ma'] / sma_20_nse_bse['20_ma_prev_30']).round(2)
sma_20_nse_bse['ratio_20ma_vol'] = (sma_20_nse_bse['20_ma_vol'] / sma_20_nse_bse['20_ma_vol_prev_30']).round(2)
sma_20_nse_bse['is_ltp_greater'] = np.where((sma_20_nse_bse['lastPrice'] > sma_20_nse_bse['20_ma']), 1, 0)
sma_20_nse_bse['secWiseDelPosDate'] = sma_20_nse_bse['secWiseDelPosDate'].dt.strftime('%d-%m-%Y')
# sma_20_nse_bse.to_csv(r'D:\Program\stockdata\raw\sma_20_nse_bse.csv', index=None)
sma_20_nse_bse.to_csv(r'\\192.168.41.190\program\stockdata\metadata\sma_20_nse_bse.csv', index=None)


