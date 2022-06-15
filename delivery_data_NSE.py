import requests
from time import sleep
import numpy as np
import threading
from utils import *
from datetime import datetime
date_today = datetime.today()
str_date = date_today.strftime('%d%m%y')
print(str_date)

pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)


is_trial = 0
is_data_dup = 1
tries = 1
max_retries = 3
timesleep = datetime.now().strftime('%H:%M')

inp_stock_df = pd.read_csv(METADATA_DIR + '\\StockMetadata.csv')
inp_stock_df = inp_stock_df[~inp_stock_df.Symbol.isnull()]

if is_trial == 1:
    sleep_dup = 3
    inp_stock_df = inp_stock_df.head(10)
else:
    sleep_dup = 300

headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9","Accept-Encoding": "gzip, deflate, br"}


# read data from json into cpg_df_list#  Load list to frames
delivery_position_df = pd.DataFrame()
try:
    delivery_position_df = pd.read_csv(PROCESSED_DIR + '\\del_data_NSE' + '\\nse_delivery_position_' + str_date + '.csv')
    delivery_position_df['secWiseDelPosDate'] = pd.to_datetime(delivery_position_df['secWiseDelPosDate'],
                                                                   format='%d-%m-%Y %H:%M')
    delivery_position_df = delivery_position_df.drop(['Series'], axis=1)
except Exception as error:
    print("Creating File".format(error))


#do not load
if timesleep < "10:55":
    delivery_position_df = pd.DataFrame()


# check if list has value, assign df[time] from last df so both time as same and than compare on df with previous
# check if current dataframe and new df are same, if yes re-try every 300 secs
while tries <= max_retries:
    # check duplicate data
    session_count = 1
    s = requests.session()
    s.get('https://www.nseindia.com/get-quotes/derivatives?symbol=' + 'NIFTY', headers=headers)
    r = s.get('https://www.nseindia.com/api/quote-equity?symbol=INFY&section=trade_info', headers=headers).json()
    delivery_position = [r['securityWiseDP']]
    dp_df_check = pd.DataFrame(delivery_position)
    is_date_eod = dp_df_check['secWiseDelPosDate'].iloc[0]

    if is_date_eod[12:15] == "EOD":
        is_date_eod = is_date_eod[:11] + " 16:00:00"
        dp_df_check['secWiseDelPosDate'] = is_date_eod

    dp_df_check['secWiseDelPosDate'] = pd.to_datetime(dp_df_check['secWiseDelPosDate'], format='%d-%b-%Y %H:%M:%S')

    if len(delivery_position_df) > 0 and dp_df_check.loc[0].secWiseDelPosDate == \
            delivery_position_df['secWiseDelPosDate'].iloc[-1]:  # and is_trial == 0:
        print("Duplicate data. Not recording")
        sleep(sleep_dup)
        tries += 1
        continue
    else:
        is_data_dup = 0
        tries = 5 # exit loop

# global delivery_position_all_list
delivery_position_all_list = []
threads =[]


def block_deal():
    s = requests.session()
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36"}
    s.get('https://www.nseindia.com/market-data/block-deal-watch', headers=headers)

    url = 'https://www.nseindia.com/api/block-deal'
    res = s.get(url, headers=headers).json()

    block_deal_df_all = pd.DataFrame()

    for i in range(len(res['data'])):
        block_deal_df = pd.DataFrame([res['data'][i]])
        block_deal_df = block_deal_df[
            ['symbol', 'totalTradedVolume', 'lastUpdateTime', 'lastPrice', 'pchange', 'series']]
        block_deal_df = block_deal_df.rename(columns={"symbol": "Stock", "totalTradedVolume": "quantityTraded",
                                                      "lastUpdateTime": "secWiseDelPosDate", "pchange": "pChange",
                                                      "series": "Series"})
        block_deal_df['deliveryToTradedQuantity'] = 100
        block_deal_df['deliveryQuantity'] = block_deal_df['quantityTraded']
        block_deal_df['vwap'] = block_deal_df['lastPrice']
        block_deal_df_all = pd.concat([block_deal_df_all, block_deal_df])

    return block_deal_df_all


def start_thread(Stock, session_count,index,s):
    print(Stock)
    try:

        url = 'https://www.nseindia.com/api/quote-equity?symbol=' + Stock.replace('&', '%26')
        r_stock = s.get(url, headers=headers).json()
        df_price = pd.DataFrame([r_stock['priceInfo']])
        df_price = df_price[['lastPrice', 'pChange','vwap']]

        df_metadata = r_stock['metadata']['series']
        if df_metadata == 'BE':
            url_del = 'https://www.nseindia.com/api/quote-equity?symbol=' + Stock.replace('&','%26') + '&section=trade_info'
            r = s.get(url_del, headers=headers).json()
            delivery_position = [r['marketDeptOrderBook']['tradeInfo']['totalTradedVolume']]
            delivery_position_df_now = pd.DataFrame()
            delivery_position_df_now['Stock'] = [Stock]
            delivery_position_df_now['quantityTraded'] = delivery_position
            delivery_position_df_now['deliveryQuantity'] = delivery_position
            delivery_position_df_now['deliveryToTradedQuantity'] = 100
            delivery_position_df_now['secWiseDelPosDate'] = is_date_eod

        else:
            url_del = 'https://www.nseindia.com/api/quote-equity?symbol=' + Stock.replace('&', '%26')+ '&section=trade_info'
            r = s.get(url_del , headers=headers).json()
            delivery_position = [r['securityWiseDP']]
            delivery_position_df_now = pd.DataFrame(delivery_position)

            replace_date_eod = delivery_position_df_now['secWiseDelPosDate'].iloc[0]
            if replace_date_eod[12:15] == "EOD":
                replace_date_eod = replace_date_eod[:11] + " 16:00:00"
                delivery_position_df_now['secWiseDelPosDate'] = replace_date_eod

            delivery_position_df_now['Stock'] = Stock
            delivery_position_df_now = delivery_position_df_now.drop(['seriesRemarks'], axis=1)

        bulkBlockDeals = [r['bulkBlockDeals']]
        bulkBlockDeals_df = pd.DataFrame(bulkBlockDeals)

        delivery_position_df_now =  pd.concat([df_price, delivery_position_df_now], axis=1, sort=False)
        delivery_position_df_now = pd.concat([bulkBlockDeals_df, delivery_position_df_now], axis=1, sort=False)
        delivery_position_all_list.append(delivery_position_df_now.to_dict('records'))

    except Exception as e:
        print(Stock, "is wrong, error" ,e)


if is_data_dup == 0:
    index = 0
    for Symbol in inp_stock_df['Symbol'].to_list():
        t1 = threading.Thread(target=start_thread, args=(Symbol,session_count,index,s))
        t1.start()
        index = index + 1
        threads.append(t1)
        if index%30 == 0:
            sleep(1)
        if index %200==0:
            s = requests.session()
            s.get('https://www.nseindia.com/get-quotes/derivatives?symbol=' + 'NIFTY', headers=headers)

    for process in threads:
        process.join()

    delivery_position_all_df = pd.DataFrame()
    for item in delivery_position_all_list:
        delivery_position_all_df = pd.concat([delivery_position_all_df, pd.DataFrame(item)])

    # remove any EOD
    delivery_position_all_df = delivery_position_all_df[~(delivery_position_all_df['secWiseDelPosDate'].str[12:15] == 'EOD')]
    delivery_position_all_df = delivery_position_all_df[delivery_position_all_df.secWiseDelPosDate != '-']
    delivery_position_all_df['secWiseDelPosDate'] = pd.to_datetime(delivery_position_all_df['secWiseDelPosDate'],
                                                                   format='%d-%b-%Y %H:%M:%S')

    if not delivery_position_all_df.empty:
        delivery_position_all_df = pd.concat([delivery_position_df, delivery_position_all_df])

    delivery_position_all_df['filter_date'] = delivery_position_all_df['secWiseDelPosDate'].dt.strftime('%d-%m-%Y')
    delivery_position_all_df['filter_date'] = pd.to_datetime(delivery_position_all_df['filter_date'],
                                                                   format='%d-%m-%Y')
    print(delivery_position_all_df['filter_date'].max(), 'this one')
    delivery_position_all_df = delivery_position_all_df[
        delivery_position_all_df['filter_date'] == delivery_position_all_df['filter_date'].max()]
    delivery_position_all_df.drop(['filter_date'], inplace=True, axis=1)

    # dump data into csv
    delivery_position_all_df = delivery_position_all_df[delivery_position_all_df['quantityTraded']>0]
    delivery_position_all_df = delivery_position_all_df.merge(inp_stock_df,left_on='Stock',right_on='Symbol',how='inner')
    # delivery_position_all_df['deliveryQuantity'] = np.where(delivery_position_all_df['Series'] == ' BE',delivery_position_all_df['quantityTraded'],delivery_position_all_df['deliveryQuantity'])
    delivery_position_all_df['vwap'] = np.where(delivery_position_all_df['Series'] == ' SM',delivery_position_all_df['lastPrice'],delivery_position_all_df['vwap'])
    delivery_position_all_df = delivery_position_all_df[['Stock','secWiseDelPosDate','lastPrice','pChange','vwap','quantityTraded','deliveryQuantity','deliveryToTradedQuantity','Series']]
    delivery_position_all_df['secWiseDelPosDate'] = delivery_position_all_df['secWiseDelPosDate'].dt.strftime('%d-%m-%Y %H:%M')
    delivery_position_all_df.to_csv(PROCESSED_DIR + '\\nse_delivery_position.csv', index=False)

    delivery_position_all_df.to_csv(PROCESSED_DIR + '\\del_data_NSE' + '\\nse_delivery_position_' + str_date + '.csv', index=False)


if timesleep < "19:55":
    # call and concat block deal data
    block_deal_df = block_deal()
    if not block_deal_df.empty:
        print('today block deal data')
        block_deal_df = block_deal_df[
            ["Stock", "secWiseDelPosDate", "lastPrice", "pChange", "vwap", "quantityTraded", "deliveryQuantity",
             "deliveryToTradedQuantity", "Series"]]
        block_deal_df['secWiseDelPosDate'] = pd.to_datetime(block_deal_df['secWiseDelPosDate'], format='%d-%b-%Y %H:%M:%S')
        block_deal_df['secWiseDelPosDate'] = block_deal_df['secWiseDelPosDate'].dt.strftime('%d-%m-%Y %H:%M')
        block_deal_df.to_csv(PROCESSED_DIR + '\\del_data_NSE' + '\\nse_delivery_position_' + str_date + '.csv', index=False, header=False, mode='a')

