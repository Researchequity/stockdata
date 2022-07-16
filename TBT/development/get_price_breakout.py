import pandas as pd
from collections import Counter
from datetime import datetime
import getpass
import numpy as np
import os

# import warnings
# warnings.simplefilter(action="ignore",category=warnings)


#path = 'Z:\\'
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)

#Change date for file read  and comment date from today
date = '20210908'
# date = ''.join(str(datetime.today().date()).split('-'))

path = '/home/workspace/aggregate/'
python_ankit = '/home/workspace/production/python_ankit/'
aggregate = '/home/workspace/aggregate/'
tradewatch = '/home/workspace/aggregate/tradewatch/'
print(date)
hit = 20
read_row = 0
def get_breakout_in_hv():
    filepath = aggregate + 'tradeWatch_historical_{}.csv'.format(date)
    ohlc_filepath = aggregate + 'ohlc_{}.csv'.format(date)

    division_factor = 375
    df_norman_trd = pd.read_csv(python_ankit + 'Average.csv')
    df_norman_trd['normean_quantity'] = df_norman_trd['vol_norm_mean'] // division_factor  # mean_quantity
    df_norman_trd['normean_trd'] = df_norman_trd['trd_norm_mean'] // division_factor
    df_norman_trd = df_norman_trd[['Stock', 'normean_quantity', 'normean_trd']]
    df_norman_trd.rename(columns={'Stock': 'Symbol'}, inplace=True)

    df_5_groupby = pd.read_csv(filepath, header=None)  # keep same
    df_5_groupby.columns = ['Date', 'token', 'Symbol', 'row_count', 'sum_qty', 'nbuy_count', 'nsell_count',
                            'vwap_min',
                            'nbuy_qty', 'nsell_qty']
    df_5_groupby['Date'] = pd.to_datetime(df_5_groupby['Date'])

    OPENING_DATA_FILE = os.path.join(tradewatch, "openingdata_{}.csv".format(date))
    open_data = pd.read_csv(OPENING_DATA_FILE, header=None)
    open_data.rename(columns={0: 'token', 1: 'o_price'}, inplace=True)

    df_6 = pd.merge(df_5_groupby, open_data, on=['token'])
    df_6 = pd.merge(df_6, df_norman_trd, on='Symbol')
    df_6 = df_6[
        ["Date", "token", "normean_quantity", "normean_trd", "nbuy_count", "nsell_count", "nbuy_qty", "nsell_qty",
         "row_count", "sum_qty", "o_price", "vwap_min", "Symbol"]]

    stockmetadata_df = pd.read_csv(python_ankit + 'StockMetadata.csv')
    stockmetadata_df = stockmetadata_df[['Symbol', 'MarketCap']]
    df_6 = df_6.merge(stockmetadata_df, on='Symbol', how='left')

    df_6['r_qty'] = df_6['sum_qty'] // df_6['normean_quantity']
    df_6['r_trade'] = df_6['row_count'] // df_6['normean_trd']
    df_6['BUY'] = df_6['nbuy_qty'] // df_6['nsell_qty']
    df_6.dropna(subset=['Symbol'], inplace=True)

    # calc vwap
    df_ohlc = pd.read_csv(ohlc_filepath, header=None)[[0, 1, 6, 7]]
    df_ohlc[0] = pd.to_datetime(df_ohlc[0])
    df_ohlc[["vwap_all"]] = df_ohlc.groupby([1])[6].cumsum(axis=0) // df_ohlc.groupby([1])[7].cumsum(axis=0)

    df_ohlc = df_ohlc[[0, 1, "vwap_all"]]
    df_ohlc.columns = ['Date', 'token', 'vwap_all']
    df_6 = pd.merge(df_6, df_ohlc, on=['Date', 'token'])
    df_6_buy = df_6[df_6['vwap_min'] > df_6['vwap_all']].copy()

    # 3 conditions row_count and o_price and qty > x * mean
    cond_b = ((df_6_buy['row_count'] >= 100) & (df_6_buy['vwap_min'] > df_6_buy['o_price'])
              & ((df_6_buy['sum_qty'] > 4 * df_6_buy['normean_quantity']) & (df_6_buy['MarketCap'] == 'LARGE')
                 | (df_6_buy['sum_qty'] > 4 * df_6_buy['normean_quantity']) & (df_6_buy['MarketCap'] == 'Mid')
                 | (df_6_buy['sum_qty'] > 8 * df_6_buy['normean_quantity']) & (df_6_buy['MarketCap'] == 'Small')
                 | (df_6_buy['sum_qty'] > 15 * df_6_buy['normean_quantity']) & (df_6_buy['MarketCap'] == 'VSM')))

    df_6_buy['repeat_b'] = np.where(cond_b, 1, 0)
    df_6_buy['repeat_bc'] = df_6_buy.groupby(["token"]).agg({'repeat_b': ['cumsum']})
    df_6_buy = df_6_buy[df_6_buy['repeat_bc'] > 0]
    df_6_buy['mean_price'] = df_6_buy['vwap_min']
    df_6_buy['SYMBOL'] = df_6_buy['Symbol']

    df_6_buy_min_count_hit = df_6_buy[df_6_buy['repeat_bc']==hit].copy()
    df_6_buy_min_count_hit = df_6_buy_min_count_hit.groupby(['token']).agg({'Date': ['first'], 'repeat_bc': ['first']}).reset_index()
    df_6_buy_min_count_hit.columns = [['token','date_hit','repeat_bc_hit']]
    df_6_buy_min_count_hit.to_csv('/home/workspace/aggregate/R3_breakout/stream_hitbhav_' + date + '.csv', index=False)

    df_6_buy = df_6_buy.groupby(['token', 'SYMBOL']).agg({'Date': ['first'], 'mean_price': ['first'], 'repeat_bc': ['max'], 'MarketCap':['max']}).reset_index()
    df_6_buy.columns = [['token', 'SYMBOL', 'date', 'mean_price', 'repeat_bc','MarketCap']]
    df_6_buy[['date', 'token', 'SYMBOL', 'mean_price', 'MarketCap','repeat_bc']].to_csv('/home/workspace/aggregate/R3_breakout/streamR3bhav_' + date + '.csv', index=False)

def get_R3_breakout_in_hv():
    num_trades = pd.read_csv(path + 'stream_op_' + date + '.csv', header=None)

    num_trades.columns = ['Date', 'token', 'normean_quantity', 'sum_qty', 'normean_trd', 'row_count', 'mean_price',
                          'o_price', 'BUY', 'repeat', 'r_qty', 'SYMBOL']
    num_trades['Date'] = pd.to_datetime(num_trades['Date'])

    # from dateutil.relativedelta import relativedelta
    # today = datetime.today().date() - relativedelta(days=1)
    # TODAY_DAY= today.day
    # TODAY_MONTH= today.month
    # TODAY_YEAR= today.year
    # num_trades = num_trades[num_trades['Date'] >datetime(day=TODAY_DAY,month=TODAY_MONTH,year=TODAY_YEAR,hour=10,minute=35,second=0)]

    #df_ohlc_eod = pd.read_csv(path + 'ohlcdaily.csv')

    #num_trade_R3 = pd.merge(num_trades, df_ohlc_eod, on='token', how='inner')

    #df_stockmetadata = pd.read_csv(python_ankit + 'StockMetadata.csv')[['token','MarketCap']]
    #num_trade_R3 = pd.merge(num_trade_R3, df_stockmetadata, on='token', how='inner')
    #num_trade_R3[['Date', 'token', 'SYMBOL', 'R3','mean_price','MarketCap']].to_csv('/home/workspace/aggregate/R3_breakout/streamR3bhav_' + date + '.csv', index=False)

    #if not num_trade_R3.empty:
        #print(num_trade_R3[['Date', 'token', 'SYMBOL', 'R3', 'mean_price']].drop_duplicates())

def get_R3_analyzed():
    # get ohlc data
    ohlc_df = pd.read_csv(path + 'ohlc_' +date + '.csv',header=None)
    ohlc_df.columns = ['date','token','open','high','low','close','value','qty']
    ohlc_df['date'] = pd.to_datetime(ohlc_df['date'])

    # get all high volume stream data where price in above R3
    streamhv_df = pd.read_csv(path + 'R3_breakout/streamR3bhav_' +date + '.csv')
    streamhv_df.columns = ['date','token','SYMBOL','mean_price','MarketCap','repeat_bc']
    streamhv_df = streamhv_df[(streamhv_df['repeat_bc'] >= hit)]

    streamhv_hit_df = pd.read_csv(path + 'R3_breakout/stream_hitbhav_' +date + '.csv')
    streamhv_df = pd.merge(streamhv_df, streamhv_hit_df, on=['token'], how='inner')

    # get first instance of day in high volume stream data where price in above R3
    # streamhv_df = streamhv_df.groupby(['token','SYMBOL']).agg({'Date':['first'],'mean_price':['first'],'repeat_bc':['max']}).reset_index()
    # streamhv_df.columns = [['token','SYMBOL','date','mean_price','repeat_bc']]

    #streamhv_df=streamhv_df[streamhv_df['SYMBOL']=='WELSPUNIND']

    up_percnt_chn_after_buy =0

    if os.path.exists(path + 'R3_breakout/pnl_' + date + '.csv'):
        buy_df_old = pd.read_csv(path + 'R3_breakout/pnl_' + date + '.csv')
        buy_df_old['date_30minR3'] = pd.to_datetime(buy_df_old['date_30minR3'] )
    else:
        buy_df_old = pd.DataFrame()

    buy_df = pd.DataFrame()
    buy_df = streamhv_df[['token','SYMBOL','date','MarketCap','repeat_bc','date_hit','repeat_bc_hit']]
    buy_df['price_percnt_rise_f_R3'] = 0
    buy_df['date_R3'] = 0
    buy_df['buy_date'] = 0
    buy_df['R3_price'] = 0
    buy_df['high_25m_Entry'] = 0
    buy_df['IsBought'] = 0
    buy_df['IsSLTrigger'] = 0
    buy_df['IsSold'] = 0
    buy_df['up_percnt_chn_after_buy'] = 0
    buy_df['down_percnt_chn_after_buy'] = 0
    

    for ind in streamhv_df.index:

        # assign each row value to variable

        symbol = streamhv_df['SYMBOL'][ind]
        token = streamhv_df['token'][ind]
        high_25m_Entry = streamhv_df['mean_price'][ind]
        date_R3 = pd.to_datetime(streamhv_df['date'][ind])
        date_30R3 = pd.to_datetime(streamhv_df['date_hit'][ind]) #+ pd.offsets.Minute(180)
        bought = 0
        stoptrigger = 0
        sold = 0
        buy_date = 0

        # get next 25 min ohlc price action
        hv_token_ohlc_df = ohlc_df[ohlc_df['token'] == token]
        hv_25min_ohlc_df = hv_token_ohlc_df[hv_token_ohlc_df['date'] <= date_30R3]
        stock_R3_price = hv_25min_ohlc_df['high'].max().item()

        hv_25min_ohlc_df = hv_25min_ohlc_df[hv_25min_ohlc_df['date'] >= date_R3]

        # get next 25 min ohlc price action
        hv_after25min_ohlc_df = hv_token_ohlc_df[hv_token_ohlc_df['date'] > date_30R3]
        high_price_after_buy = hv_after25min_ohlc_df['high'].max()
        price_percnt_rise_f_R3 = round((high_price_after_buy - stock_R3_price) * 100 / stock_R3_price, 2)

        # get if bougth
        if high_price_after_buy > stock_R3_price and bought == 0: #and price_percnt_rise_f_R3 < 2:

            up_percnt_chn_after_buy = round((high_price_after_buy - stock_R3_price) * 100 / stock_R3_price, 1)
            buy_entry_df = hv_token_ohlc_df[hv_token_ohlc_df['high']>stock_R3_price].head(1).reset_index()
            price_buy = buy_entry_df.iloc[0]['low']
            if price_buy < stock_R3_price:
                bought = 1

            if len(buy_entry_df):
                buy_date = buy_entry_df.iloc[0]['date']

        # get target hit or sl trigger
        if bought == 1 and len(buy_entry_df):
            hv_buy_ohlc_df = hv_token_ohlc_df[hv_token_ohlc_df['date'] > buy_date].copy()
            hv_buy_ohlc_df['entry_high_perct'] = round((hv_buy_ohlc_df['high'] - stock_R3_price) * 100 / stock_R3_price,2)
            hv_buy_ohlc_df['entry_low_perct'] = round(((hv_buy_ohlc_df['low'] - stock_R3_price) * 100) / stock_R3_price,2)
            hv_buy_ohlc_df = hv_buy_ohlc_df[(hv_buy_ohlc_df['entry_high_perct']>1) | (hv_buy_ohlc_df['entry_low_perct']<-1)]

            if len(hv_buy_ohlc_df):
                hv_buy_ohlc_df_first_high = hv_buy_ohlc_df['entry_high_perct'].head(1).item()
                hv_buy_ohlc_df_first_low = hv_buy_ohlc_df['entry_low_perct'].head(1).item()

                if hv_buy_ohlc_df_first_low < -1:
                    stoptrigger = 1
                if hv_buy_ohlc_df_first_high > 1:
                    sold = 1


        if price_percnt_rise_f_R3 < 3 or 0==0:

            buy_df['price_percnt_rise_f_R3'] = np.where((buy_df['token'] == token), price_percnt_rise_f_R3,
                                                        buy_df['price_percnt_rise_f_R3'])
            # buy_df['date_R3'] = np.where((buy_df['token'] == token),date_R3,buy_df['date_R3'])
            buy_df['buy_date'] = np.where((buy_df['token'] == token), buy_date, buy_df['buy_date'])
            buy_df['R3_price'] = np.where((buy_df['token'] == token), stock_R3_price, buy_df['R3_price'])
            buy_df['high_25m_Entry'] = np.where((buy_df['token'] == token), high_25m_Entry, buy_df['high_25m_Entry'])
            buy_df['IsBought'] = np.where((buy_df['token'] == token), bought, buy_df['IsBought'])
            buy_df['IsSLTrigger'] = np.where((buy_df['token'] == token), stoptrigger, buy_df['IsSLTrigger'])
            buy_df['up_percnt_chn_after_buy'] = np.where((buy_df['token'] == token), up_percnt_chn_after_buy,
                                                         buy_df['up_percnt_chn_after_buy'])
            buy_df['IsSold'] = np.where((buy_df['token'] == token), sold, buy_df['IsSold'])


        else:
            buy_df['price_percnt_rise_f_R3'] = np.where((buy_df['token'] == token), price_percnt_rise_f_R3,
                                                        buy_df['price_percnt_rise_f_R3'])
            buy_df['buy_date'] = np.where((buy_df['token'] == token), buy_date, buy_df['buy_date'])
            print('Out Range', symbol, 'from R3 rise in %', price_percnt_rise_f_R3)

    buy_df = buy_df.sort_values(['date'])
    buy_df.reset_index(inplace=True)
    #buy_df.to_csv('/home/workspace/aggregate/R3_breakout/' + 'pnl_' + date + '.csv', index=False)
    buy_df['Date_only'] = date
    buy_df.to_csv('/home/workspace/aggregate/R3_breakout/' + 'pnl_1_perct_both_b_s.csv', index=False, mode='a')

    buy_df = buy_df[buy_df_old.eq(buy_df).all(axis=1) == False]
    if not buy_df.empty:
        print(buy_df)
        print(buy_df['IsBought'].sum(), buy_df['IsSold'].sum(), buy_df['IsSLTrigger'].sum())


if __name__ == '__main__':
    while 0 == 0:
        if 1==1:
            for i in range(180):
                print(i)
                from dateutil.relativedelta import relativedelta
                today = datetime.today().date() - relativedelta(days=i)
                date = ''.join(str(today).split('-'))
                try:
                    # get_R3_breakout_in_hv()
                    get_breakout_in_hv()
                    get_R3_analyzed()
                except Exception as e:
                    print(e)
                    continue
                i = i + 1
            import time
            time.sleep(60)
            break

