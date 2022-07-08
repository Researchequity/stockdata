import pandas as pd
from collections import Counter
from datetime import datetime
import getpass
import numpy as np
import os
import warnings
warnings.simplefilter(action="ignore",category=warnings)

path = '/home/workspace/aggregate/'
#path = 'Z:\\'
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)

#Change date for file read  and comment date from today
date = '20210409'
date = ''.join(str(datetime.today().date()).split('-'))
print(date)

read_row = 0


def get_R3_breakout_in_hv():
    num_trades = pd.read_csv(path + 'stream_op_' + date + '.csv', header=None)
    #read_row = read_row + len(num_trades)

    num_trades.columns = ['Date', 'token', 'normean_quantity', 'sum_qty', 'normean_trd', 'row_count', 'mean_price',
                          'o_price', 'BUY', 'repeat', 'r_qty', 'SYMBOL']
    # num_trades['Date'] = num_trades['Date'].str[10:16]

    df_ohlc_eod = pd.read_csv(path + 'ohlcdaily.csv')

    #df_ohlc_eod['pp'] = (df_ohlc_eod['high'] + df_ohlc_eod['low'] + df_ohlc_eod['close']) // 3
    #df_ohlc_eod['R3'] = df_ohlc_eod['high'] + 2 * (df_ohlc_eod['pp'] - df_ohlc_eod['low'])

    num_trade_R3 = pd.merge(num_trades, df_ohlc_eod, on='token', how='inner')

    num_trade_R3 = num_trade_R3[num_trade_R3['mean_price'] >= num_trade_R3['R3']]

    num_trade_R3[['Date', 'token', 'SYMBOL', 'R3', 'mean_price']].to_csv('/home/workspace/aggregate/R3_breakout/streamR3bhav_' + date + '.csv', index=False)

    #if not num_trade_R3.empty:
        #print(num_trade_R3[['Date', 'token', 'SYMBOL', 'R3', 'mean_price']].drop_duplicates())

def get_R3_analyzed():
    # get ohlc data
    ohlc_df = pd.read_csv(path + 'ohlc_' +date + '.csv',header=None)
    ohlc_df.columns = ['date','token','open','high','low','close','value','qty']
    ohlc_df['date'] = pd.to_datetime(ohlc_df['date'])

    # get all high volume stream data where price in above R3
    streamhv_df = pd.read_csv(path + 'R3_breakout/streamR3bhav_' +date + '.csv')
    streamhv_df.columns = ['Date','token','SYMBOL','R3','mean_price']
    streamhv_df['R3'] = round(streamhv_df['R3'],0)

    # get first instance of day in high volume stream data where price in above R3
    streamhv_df = streamhv_df.groupby(['token','SYMBOL','R3']).agg({'Date':['first'],'mean_price':['first']}).reset_index()
    streamhv_df.columns = ['token','SYMBOL','R3','date','mean_price']
    #streamhv_df=streamhv_df[streamhv_df['SYMBOL']=='BRITANNIA']

    up_percnt_chn_after_buy =0

    if os.path.exists(path + 'R3_breakout/pnl_' + date + '.csv'):
        buy_df_old = pd.read_csv(path + 'R3_breakout/pnl_' + date + '.csv')
        buy_df_old['date_30minR3'] = pd.to_datetime(buy_df_old['date_30minR3'] )
    else:
        buy_df_old = pd.DataFrame()

        # buy_df_half = buy_df[['token','SYMBOL','R3','date']]
        # streamhv_new_df = streamhv_df[['token','SYMBOL','R3','date']]
        #
        # streamhv_new_df.set_index(['token'],inplace =True)
        # buy_df_half.set_index(['token'],inplace =True)
        #
        # # Add new R3 break out
        # append_df = streamhv_new_df[~streamhv_new_df['SYMBOL'].isin(buy_df_half['SYMBOL'])]
        # append_df.reset_index(inplace=True)
        # buy_df = buy_df.append(append_df)
    
    #else:

    buy_df = pd.DataFrame()
    buy_df = streamhv_df[['token','SYMBOL','R3','date']]
    buy_df['price_percnt_rise_f_R3'] = 0
    buy_df['date_R3'] = 0
    buy_df['date_30minR3'] = 0
    buy_df['R3_price'] = 0
    buy_df['high_25m_Entry'] = 0
    buy_df['IsBought'] = 0
    buy_df['IsSLTrigger'] = 0
    buy_df['up_percnt_chn_after_buy'] = 0
    buy_df['down_percnt_chn_after_buy'] = 0
    

    for ind in streamhv_df.index:

        # assign each row value to variable
        symbol = streamhv_df['SYMBOL'][ind]
        token = streamhv_df['token'][ind]
        date_R3 = pd.to_datetime(streamhv_df['date'][ind])
        date_30R3 = pd.to_datetime(streamhv_df['date'][ind]) + pd.offsets.Minute(25)
        stock_R3_price = round(streamhv_df['R3'][ind])

        # get next 25 min ohlc price action
        hv_token_ohlc_df = ohlc_df[ohlc_df['token'] == token]
        hv_25min_ohlc_df = hv_token_ohlc_df[hv_token_ohlc_df['date'] >= date_R3]
        hv_25min_ohlc_df = hv_25min_ohlc_df[hv_25min_ohlc_df['date'] <= date_30R3]

        high_25m_Entry = hv_25min_ohlc_df['high'].max()
        
        price_percnt_rise_f_R3 = round((high_25m_Entry - stock_R3_price) * 100 / stock_R3_price, 2)

        # get next 25 min ohlc price action
        hv_after25min_ohlc_df = hv_token_ohlc_df[hv_token_ohlc_df['date'] > date_30R3]

        high_price_after_buy = hv_after25min_ohlc_df['high'].max()

        if high_price_after_buy > high_25m_Entry and price_percnt_rise_f_R3 < 2:
            bought = 1
            up_percnt_chn_after_buy = round((high_price_after_buy - high_25m_Entry) * 100 / high_25m_Entry, 1)

        else:
            bought = 0

        low_price_after_buy = hv_after25min_ohlc_df['close'].min()
        down_percnt_chn_after_buy = round((low_price_after_buy - stock_R3_price) * 100 / stock_R3_price, 1)

        if down_percnt_chn_after_buy < -1 and bought == 1:
            Stoptrigger = 1
        else:
            Stoptrigger = 0

        # print(date_30R3)
        # print(stock_R3_price)

        if price_percnt_rise_f_R3 < 3:

            # print('In Range','BUY', token, symbol , 'Trigger at', date_R3,'R3 price',stock_R3_price ,'Gestation ended', date_30R3, 'high of day', high_25m_Entry, 'from R3 rise in %' ,price_percnt_rise_f_R3)

            buy_df['price_percnt_rise_f_R3'] = np.where((buy_df['token'] == token), price_percnt_rise_f_R3,
                                                        buy_df['price_percnt_rise_f_R3'])
            # buy_df['date_R3'] = np.where((buy_df['token'] == token),date_R3,buy_df['date_R3'])
            buy_df['date_30minR3'] = np.where((buy_df['token'] == token), date_30R3, buy_df['date_30minR3'])
            buy_df['R3_price'] = np.where((buy_df['token'] == token), stock_R3_price, buy_df['R3_price'])
            buy_df['high_25m_Entry'] = np.where((buy_df['token'] == token), high_25m_Entry, buy_df['high_25m_Entry'])
            buy_df['IsBought'] = np.where((buy_df['token'] == token), bought, buy_df['IsBought'])
            buy_df['IsSLTrigger'] = np.where((buy_df['token'] == token), Stoptrigger, buy_df['IsSLTrigger'])
            buy_df['up_percnt_chn_after_buy'] = np.where((buy_df['token'] == token), up_percnt_chn_after_buy,
                                                         buy_df['up_percnt_chn_after_buy'])
            buy_df['down_percnt_chn_after_buy'] = np.where((buy_df['token'] == token), down_percnt_chn_after_buy,
                                                           buy_df['down_percnt_chn_after_buy'])


        else:
            buy_df['price_percnt_rise_f_R3'] = np.where((buy_df['token'] == token), price_percnt_rise_f_R3,
                                                        buy_df['price_percnt_rise_f_R3'])
            buy_df['date_30minR3'] = np.where((buy_df['token'] == token), date_30R3, buy_df['date_30minR3'])
            print('Out Range', symbol, 'from R3 rise in %', price_percnt_rise_f_R3)

    buy_df = buy_df.sort_values(['date'])
    buy_df.reset_index(inplace=True)
    buy_df.to_csv('/home/workspace/aggregate/R3_breakout/' + 'pnl_' + date + '.csv', index=False)

    buy_df = buy_df[buy_df_old.eq(buy_df).all(axis=1) == False]
    if not buy_df.empty:
        print(buy_df)
    # print(buy_df_old.dtypes)
    # print(buy_df.dtypes)
    # print(buy_df_old.eq(buy_df))



if __name__ == '__main__':
    while 0 == 0:
        if 1==1:
            get_R3_breakout_in_hv()
            get_R3_analyzed()
            import time
            time.sleep(60)

