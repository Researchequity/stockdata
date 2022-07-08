import pandas as pd
from collections import Counter
from datetime import datetime
import getpass
import numpy as np
import os
from glob import glob

import warnings
#warnings.simplefilter(action="ignore",category=warnings)

path = '/home/workspace/aggregate/'
#path = 'Z:\\'
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)

#Change date for file read  and comment date from today
date = '20210412'
#date = ''.join(str(datetime.today().date()).split('-'))
print(date)

read_row = 0

large_hv_all_df =pd.DataFrame()

for file in glob(path + 'tradewatch_outputfiles_archives/stream_op_*.csv'):
    print(file)
    large_hv_df = pd.read_csv(file, header=None)
    large_hv_all_df = pd.concat([large_hv_df,large_hv_all_df])

large_hv_all_df.columns = ['date_hv', 'token', 'normean_quantity', 'sum_qty', 'normean_trd', 'row_count', 'mean_price',
                      'o_price', 'BUY', 'repeat', 'r_qty', 'symbol']
large_hv_all_df['date_hv'] = pd.to_datetime(large_hv_all_df['date_hv'])

stockmetadata = pd.read_csv('/home/workspace/production/python_ankit/StockMetadata.csv')[['token','MarketCap']]
large_hv_all_df = pd.merge(large_hv_all_df, stockmetadata, on='token', how='inner')
large_hv_all_df = large_hv_all_df[large_hv_all_df['MarketCap'] =='LARGE']
large_hv_all_df = large_hv_all_df.groupby(['token','symbol']).agg({'date_hv':['max'],'repeat':['max']}).reset_index()
large_hv_all_df.columns = ['token','symbol','date','repeat']

print(large_hv_all_df[large_hv_all_df['repeat']>50])
exit()

if 1 ==0:
    num_trades = num_trades.groupby(['token', 'symbol']).agg(
        {'date_hv': ['first'], 'normean_quantity': ['first'], 'o_price': ['first']}).reset_index()
    num_trades.columns = ['token', 'symbol', 'date_hv', 'normean_quantity', 'o_price']

    df_ohlc_min = pd.read_csv(path + 'ohlc_'+ date + '.csv',header=None)
    df_ohlc_min.columns = ['date_min','token','open','high','low','close','value','qty']

    df_ohlc_min['date_min'] = pd.to_datetime(df_ohlc_min['date_min'])

    last_min = pd.to_datetime(df_ohlc_min['date_min'].max())
    TODAY_DAY = last_min.day
    TODAY_MONTH = last_min.month
    TODAY_YEAR = last_min.year
    today_11 = datetime(day=TODAY_DAY, month=TODAY_MONTH, year=TODAY_YEAR, hour=11, minute=0, second=0)

    num_trades = num_trades[num_trades['date_hv'] <= today_11]
    df_ohlc_min = df_ohlc_min[df_ohlc_min['date_min'] <= today_11]

    num_trades_11 = pd.merge(num_trades, df_ohlc_min, on=['token'], how='inner')


    df_ohlc_min_11_max = num_trades_11.groupby(['token']).agg({'high':['max'],'qty':['sum']}).reset_index()
    df_ohlc_min_11_max.columns =['token','high','sum_qty']

    # get date of high price marked
    df_ohlc_min_11_max = pd.merge(df_ohlc_min_11_max, df_ohlc_min, on=['token','high'], how='inner')
    df_ohlc_min_11_max = df_ohlc_min_11_max.groupby(['token']).agg({'high':['first'], 'date_min':['first'],'sum_qty':['first']}).reset_index()
    df_ohlc_min_11_max.columns = ['token','high_max_11','date','qty_11']

    #df_ohlc_eod['pp'] = (df_ohlc_eod['high'] + df_ohlc_eod['low'] + df_ohlc_eod['close']) // 3
    #df_ohlc_eod['R3'] = df_ohlc_eod['high'] + 2 * (df_ohlc_eod['pp'] - df_ohlc_eod['low'])

    num_trade_11_breakout = pd.merge(num_trades, df_ohlc_min_11_max, on='token', how='inner')

    # if stock hit same price high twice take first row
    num_trade_11_breakout.sort_values(['date'], inplace=True)
    num_trade_11_breakout.to_csv('/home/workspace/aggregate/R3_breakout/stream_high_11_bhav_' + date + '.csv', index=False)

    #num_trade_11_breakout[['date', 'token', 'symbol', 'high_max_11', 'o_price','sum_qty','normean_quantity']].to_csv('/home/workspace/aggregate/R3_breakout/streamR3bhav_' + date + '.csv', index=False)

    #if not num_trade_R3.empty:
        #print(num_trade_R3[['Date', 'token', 'symbol', 'R3', 'mean_price']].drop_duplicates())

def get_R3_analyzed():
    # get ohlc data
    ohlc_df = pd.read_csv(path + 'ohlc_' +date + '.csv',header=None)
    ohlc_df.columns = ['date','token','open','high','low','close','value','qty']
    ohlc_df['date'] = pd.to_datetime(ohlc_df['date'])

    ohlc_df = ohlc_df.sort_values(['date']).reset_index(drop=True)
    ohlc_df['value_cumsum'] = ohlc_df.groupby(['token'])['value'].cumsum(axis=0)
    ohlc_df['qty_cumsum'] = ohlc_df.groupby(['token'])['qty'].cumsum(axis=0)
    ohlc_df['vwap'] = ohlc_df['value_cumsum'] // ohlc_df['qty_cumsum']
    ohlc_df.drop(['value_cumsum', 'qty_cumsum'], axis=1, inplace=True)

    ohlc_df['abv_vwap'] = np.where(ohlc_df['close'] >= ohlc_df['vwap'], 1, 0)
    ohlc_df['abv_vwap_count'] = ohlc_df.groupby(['token'])['abv_vwap'].cumsum(axis=0)
    ohlc_df.to_csv('/home/workspace/aggregate/ohlc_vwap.csv',index=None)


    # get all high volume stream data where price in above R3
    streamhv_df = pd.read_csv(path + 'R3_breakout/stream_high_11_bhav_' +date + '.csv')
    streamhv_df['normean_quantity'] = streamhv_df['normean_quantity'] * 105
    streamhv_df['qty_ratio'] = round(streamhv_df['qty_11'] / streamhv_df['normean_quantity'],2)

    stockmetadata = pd.read_csv('/home/workspace/production/python_ankit/StockMetadata.csv')[['token','MarketCap']]
    streamhv_df = pd.merge(streamhv_df, stockmetadata, on='token', how='inner')


    #streamhv_df=streamhv_df[streamhv_df['symbol']=='HIKAL']

    up_percnt_chn_after_buy =0

    if os.path.exists(path + 'R3_breakout/high_11_pnl_' + date + '.csv',):
        buy_df_old = pd.read_csv(path + 'R3_breakout/high_11_pnl_' + date + '.csv')
        buy_df_old['date_30minR3'] = pd.to_datetime(buy_df_old['date_30minR3'] )
    else:
        buy_df_old = pd.DataFrame()


    buy_df = pd.DataFrame()
    buy_df = streamhv_df[['token','symbol','high_max_11','date','qty_ratio','MarketCap']]
    buy_df['date_30minR3'] = 0
    buy_df['R3_price'] = 0
    buy_df['high_25m_Entry'] = 0
    buy_df['IsBought'] = 0
    buy_df['IsSLTrigger'] = 0
    buy_df['up_percnt_chn_after_buy'] = 0
    buy_df['down_percnt_chn_after_buy'] = 0
    buy_df['date_bought'] = 0
    buy_df['ltp'] = 0
    buy_df['count_abv_vwap'] =0
    buy_df['abv_vwap_sum'] = 0
    buy_df['close_p_buy'] = 0

    for ind in streamhv_df.index:
        date_bought = 0
        ltp = 0
        close_p_buy = 0

        last_min = pd.to_datetime(streamhv_df['date'].max())
        TODAY_DAY = last_min.day
        TODAY_MONTH = last_min.month
        TODAY_YEAR = last_min.year
        today_11 = datetime(day=TODAY_DAY, month=TODAY_MONTH, year=TODAY_YEAR, hour=11, minute=0, second=0)
        today_915 = datetime(day=TODAY_DAY, month=TODAY_MONTH, year=TODAY_YEAR, hour=9, minute=15, second=0)

        # assign each row value to variable
        symbol = streamhv_df['symbol'][ind]
        token = streamhv_df['token'][ind]
        date_R3 = pd.to_datetime(streamhv_df['date'][ind])
        MarketCap = streamhv_df['MarketCap'][ind]
        qty_ratio = streamhv_df['qty_ratio'][ind]
        date_30R3 = pd.to_datetime(streamhv_df['date'][ind]) + pd.offsets.Minute(25)
        if date_30R3 < today_11:
            date_30R3 = today_11

        stock_R3_price = round(streamhv_df['high_max_11'][ind])

        # get next 25 min ohlc price action
        hv_token_ohlc_df = ohlc_df[ohlc_df['token'] == token]

        high_25m_Entry = streamhv_df['high_max_11'][ind]
        
        price_percnt_rise_f_R3 = 0 #round((high_25m_Entry - stock_R3_price) * 100 / stock_R3_price, 2)

        count_abv_vwap = 0
        abv_vwap_sum = 0
        # get ohlc price action after buy trigger
        hv_after25min_ohlc_df = hv_token_ohlc_df[hv_token_ohlc_df['date'] > date_30R3]
        count_abv_vwap = hv_after25min_ohlc_df['abv_vwap_count'].head(1)
        count_abv_vwap = count_abv_vwap.iloc[0]

        price_trigger_ohlc_df = hv_after25min_ohlc_df[hv_after25min_ohlc_df['high'] > stock_R3_price].head(1)

        if not price_trigger_ohlc_df.empty:
            price_trigger_ohlc_df = price_trigger_ohlc_df['date']
            buy_price_trigger = price_trigger_ohlc_df.iloc[0] + pd.offsets.Minute(1)
            hv_after25min_ohlc_df = hv_token_ohlc_df[hv_token_ohlc_df['date'] > buy_price_trigger]
            if not hv_after25min_ohlc_df.empty:
                count_abv_vwap = hv_after25min_ohlc_df['abv_vwap_count'].head(1)
                count_abv_vwap = count_abv_vwap.iloc[0]



        hv_after25min_ohlc_df = hv_after25min_ohlc_df[hv_after25min_ohlc_df['date'] > date_30R3]


        if not hv_after25min_ohlc_df.empty:
            date_bought = hv_after25min_ohlc_df['date'].min()
            abv_vwap_sum = (date_bought - today_915).total_seconds() / 60
            ltp = hv_after25min_ohlc_df['close'].iloc[-1] #[hv_after25min_ohlc_df.index[-1]]
        else:
            date_bought = 0
            ltp = 0

        #num_trade_11_breakout = pd.merge(num_trades_11, df_ohlc_min_11_max, on='token', how='inner')
        #print(hv_after25min_ohlc_df)
        close_p_buy = round((ltp - high_25m_Entry) * 100 / high_25m_Entry, 1)
        high_price_after_buy = hv_after25min_ohlc_df['high'].max()


        if high_price_after_buy > high_25m_Entry and (count_abv_vwap/abv_vwap_sum > 0.6 or qty_ratio> 10 ) and MarketCap !='LARGE' : # bought = 1
            bought = 1
            up_percnt_chn_after_buy = round((high_price_after_buy - high_25m_Entry) * 100 / high_25m_Entry, 1)
            hv_sl_ohlc_df = hv_after25min_ohlc_df[hv_after25min_ohlc_df['high'] >= high_price_after_buy]
            date_stoptrigger = hv_sl_ohlc_df['date'].min()
        else:
            bought = 0

        low_price_after_buy = hv_after25min_ohlc_df['low'].min()
        down_percnt_chn_after_buy = round((low_price_after_buy - stock_R3_price) * 100 / stock_R3_price, 1)

        if down_percnt_chn_after_buy < -1 and bought == 1:
            Stoptrigger = 1
            hv_sl_ohlc_df = hv_after25min_ohlc_df[hv_after25min_ohlc_df['low'] < low_price_after_buy]
            date_stoptrigger = hv_sl_ohlc_df['date'].min()
        else:
            Stoptrigger = 0


        if price_percnt_rise_f_R3 < 3:


            #buy_df['price_percnt_rise_f_R3'] = np.where((buy_df['token'] == token), price_percnt_rise_f_R3,buy_df['price_percnt_rise_f_R3'])

            buy_df['date_30minR3'] = np.where((buy_df['token'] == token), date_30R3, buy_df['date_30minR3'])
            buy_df['R3_price'] = np.where((buy_df['token'] == token), stock_R3_price, buy_df['R3_price'])
            buy_df['high_25m_Entry'] = np.where((buy_df['token'] == token), high_25m_Entry, buy_df['high_25m_Entry'])
            buy_df['IsBought'] = np.where((buy_df['token'] == token), bought, buy_df['IsBought'])
            buy_df['ltp'] = np.where((buy_df['token'] == token), ltp, buy_df['ltp'])
            buy_df['date_bought'] = np.where((buy_df['token'] == token), date_bought, buy_df['date_bought'])
            buy_df['abv_vwap_sum'] =  np.where((buy_df['token'] == token), abv_vwap_sum, buy_df['abv_vwap_sum'])
            buy_df['count_abv_vwap'] = np.where((buy_df['token'] == token), count_abv_vwap, buy_df['count_abv_vwap'])
            buy_df['close_p_buy'] = np.where((buy_df['token'] == token), close_p_buy, buy_df['close_p_buy'])

            buy_df['IsSLTrigger'] = np.where((buy_df['token'] == token), Stoptrigger, buy_df['IsSLTrigger'])
            buy_df['up_percnt_chn_after_buy'] = np.where((buy_df['token'] == token), up_percnt_chn_after_buy,
                                                         buy_df['up_percnt_chn_after_buy'])
            buy_df['down_percnt_chn_after_buy'] = np.where((buy_df['token'] == token), down_percnt_chn_after_buy,
                                                           buy_df['down_percnt_chn_after_buy'])


        else:
            #buy_df['price_percnt_rise_f_R3'] = np.where((buy_df['token'] == token), price_percnt_rise_f_R3, buy_df['price_percnt_rise_f_R3'])
            buy_df['date_30minR3'] = np.where((buy_df['token'] == token), date_30R3, buy_df['date_30minR3'])
            print('Out Range', symbol, 'from R3 rise in %', price_percnt_rise_f_R3)

    buy_df = buy_df.sort_values(['date'])


    buy_df.reset_index(inplace=True)
    buy_df.to_csv('/home/workspace/aggregate/R3_breakout/high_11_pnl_' + date + '.csv', index=False)

    buy_df = buy_df[buy_df_old.eq(buy_df).all(axis=1) == False]
    if not buy_df.empty:
        print(buy_df[buy_df['IsBought']==1])
    # print(buy_df_old.dtypes)
    # print(buy_df.dtypes)
    # print(buy_df_old.eq(buy_df))ccams




