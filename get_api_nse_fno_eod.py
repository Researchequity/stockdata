import pandas as pd
import requests
import os, os.path
import xlwings as xw
import win32com.client
import warnings
warnings.simplefilter(action="ignore", category=Warning)
import time
import numpy as np
from utils import *
from time import sleep
import threading

threads = []
headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br"
    }
LOG_FILE_NAME = str(LOG_DIR)+"\\get_api_nse_fno_eod.log"
df_itm_opt_wrt_all = []
df_first_row_all = []
df_final_all = []
all_stock_df = []
index_expiry = pd.DataFrame()


def last_day_of_month(any_day):
    # The day 28 exists in every month. 4 days later, it's always next month
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
    # subtracting the number of the current day brings us back one month
    return next_month - datetime.timedelta(days=next_month.day)


def start_thread(StockSymbol,index,s):
    try:
        url = 'https://www.nseindia.com/api/quote-derivative?symbol=' + StockSymbol.replace('&', '%26')
        r = s.get(url, headers=headers).json()

        meta_trade_all_df = pd.DataFrame()
        x = 1
        for data in r['stocks']:
            metadata_values = data['metadata']
            tradeInfo_values = data['marketDeptOrderBook']['tradeInfo']
            metadata_values.update(tradeInfo_values)
            meta_trade_df = pd.DataFrame([metadata_values])
            meta_trade_all_df = pd.concat([meta_trade_all_df, meta_trade_df])

            if x == 1:
                meta_trade_first_df = pd.DataFrame(meta_trade_all_df)

                date_exp = meta_trade_first_df['expiryDate']
                meta_trade_first_df.drop(
                    ['closePrice', 'highPrice', 'highPrice', 'identifier', 'instrumentType', 'lastPrice',
                     'numberOfContractsTraded', 'openPrice', 'pchangeinOpenInterest'
                        , 'premiumTurnover', 'prevClose', 'prevClose', 'totalTurnover', 'tradedVolume', 'value',
                     'vmap', 'lowPrice', 'changeinOpenInterest', 'change', 'openInterest'
                        , 'optionType', 'pChange'], inplace=True, axis=1)

                x += 1

        underlying_value = r['underlyingValue']
        atm_strike = sorted([[round(abs(underlying_value - i), 2), i] for i in r['strikePrices']])[0][1]

        meta_trade_all_df['strikePrice_ATM'] = atm_strike
        meta_trade_first_df['strikePrice'] = atm_strike
        strike_price_new = atm_strike

        # adding columns to dataframe
        meta_trade_all_df['STOCK'] = StockSymbol
        meta_trade_first_df['STOCK'] = StockSymbol
        meta_trade_all_df['underlying value'] = underlying_value

        date_today = r['fut_timestamp']

        meta_trade_all_df['date_today'] = date_today
        meta_trade_all_df['date_today'] = pd.to_datetime(meta_trade_all_df['date_today'],
                                                         format='%d-%b-%Y %H:%M:%S')
        global sub_string_date
        sub_string_date = date_today[0:11]
        sub_string_date = datetime.datetime.strptime(sub_string_date, '%d-%b-%Y').strftime('%Y_%m_%d')

        max_date = meta_trade_all_df.date_today.max()
        max_date = max_date.date()

        # max_date = datetime.strptime(max_date, '%d-%b-%Y')
        ##today_df = meta_trade_all_df[(meta_trade_all_df['date_today'] == max_date)]
        ##today_df = pd.DataFrame(today_df)

        # thursday concept
        meta_trade_all_df['expiryDate'] = pd.to_datetime(meta_trade_all_df['expiryDate'], format='%d-%b-%Y')
        min_exp = meta_trade_all_df['expiryDate'].min()
        min_exp = min_exp.date()
        global EXP_date
        if max_date == min_exp:
            EXP_unique = meta_trade_all_df['expiryDate'].unique()
            EXP_unique = pd.DataFrame(EXP_unique)
            EXP_unique[0] = pd.to_datetime(EXP_unique[0])
            EXP_unique.sort_values(by=0, ascending=False, inplace=True)
            EXP_date = EXP_unique[0].iloc[-2]
        else:
            EXP_unique = meta_trade_all_df['expiryDate'].unique()
            EXP_unique = pd.DataFrame(EXP_unique)
            EXP_unique[0] = pd.to_datetime(EXP_unique[0])
            EXP_unique.sort_values(by=0, ascending=False, inplace=True)
            EXP_date = EXP_unique[0].iloc[-1]
        # print(EXP_date, 'this one')
        # if StockSymbol == 'NIFTY':
        #     global index_expiry
        #     index_expiry = EXP_date
        # else:
        #     global stock_expiry
        #     stock_expiry = EXP_date

        if (meta_trade_all_df['instrumentType'].iloc[0] == 'Stock Futures') | (meta_trade_all_df['instrumentType'].iloc[0] == 'Stock Options'):
            global stock_expiry
            stock_expiry = EXP_date
        else:
            global index_expiry
            index_expiry = EXP_date

        # create historical
        meta_trade_all_df.drop(['change', 'pchangeinOpenInterest', 'premiumTurnover', 'closePrice'
                                   , 'totalTurnover', 'tradedVolume', 'value'], inplace=True, axis=1)
        meta_trade_all_df = meta_trade_all_df.rename(
            columns={"STOCK": "STOCK", "changeinOpenInterest": "CHG_IN_OI", "date_today": "date_today",
                     "expiryDate": "expiryDate", "highPrice": "HIGH",
                     "identifier": "identifier", "lastPrice": "LTP", "lowPrice": "LOW", "marketLot": "LOT",
                     "openInterest": "OPEN_INT", "openPrice": "OPEN"
                , "optionType": "optionType", "pChange": "pChange", "prevClose": "CLOSE",
                     "strikePrice": "strikePrice"
                , "vmap": "VWAP", "numberOfContractsTraded": "CONTRACTS", "underlying value": "Spot"})
        meta_trade_all_df = meta_trade_all_df[
            ['STOCK', 'expiryDate', 'optionType', 'strikePrice', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'LTP', 'pChange',
             'CONTRACTS', 'VWAP', 'OPEN_INT', 'CHG_IN_OI', 'LOT', 'Spot', 'date_today',
             'identifier', 'instrumentType']]

        meta_trade_all_df['CHG_IN_OI'] = meta_trade_all_df['CHG_IN_OI'] * meta_trade_all_df['LOT']
        meta_trade_all_df['%_of_OI'] = meta_trade_all_df['CHG_IN_OI'] / meta_trade_all_df['OPEN_INT']
        meta_trade_all_df['OPEN_INT'] = meta_trade_all_df['OPEN_INT'] * meta_trade_all_df['LOT']
        meta_trade_all_csv = pd.DataFrame(meta_trade_all_df[(meta_trade_all_df['CONTRACTS'] >= 100)])
        # all_stock_df = pd.concat([all_stock_df, meta_trade_all_csv])
        all_stock_df.append(meta_trade_all_csv)

        # calculate and add strike price difference to csv
        eom_EXP_date = last_day_of_month(EXP_date)

        meta_trade_all_df = meta_trade_all_df[(meta_trade_all_df['expiryDate'] <= eom_EXP_date)]
        first_row_spd_df = meta_trade_all_df[
            (meta_trade_all_df['strikePrice'] >= strike_price_new) & (meta_trade_all_df['optionType'] == 'Call')]

        meta_trade_unique = first_row_spd_df['strikePrice'].unique()
        meta_trade_unique = pd.DataFrame(meta_trade_unique)
        meta_trade_unique.columns = ['strikePrice']
        meta_trade_unique.sort_values(by=['strikePrice'], inplace=True)
        strike_price_one = meta_trade_unique['strikePrice'].iloc[0]
        strike_price_two = meta_trade_unique['strikePrice'].iloc[1]

        meta_trade_first_df['expiryDate'] = pd.to_datetime(meta_trade_first_df['expiryDate'])
        meta_trade_first_df['Strike_price_diff'] = strike_price_two - strike_price_one
        meta_trade_first_df.columns = ['expiryDate', 'strikePrice_ATM', 'marketLot', 'STOCK', 'Strike_price_diff']
        # df_first_row_all = pd.concat([df_first_row_all, meta_trade_first_df])
        df_first_row_all.append(meta_trade_first_df)
        # df_first_row_all.drop(['strikePrice_ATM'], inplace=True, axis=1)

        # strike price top down
        Strike_price_diff = meta_trade_first_df['Strike_price_diff']
        Strike_price_diff = Strike_price_diff[0] * 10
        strikePrice_ATM = meta_trade_first_df['strikePrice_ATM']
        strikePrice_ATM_top = strikePrice_ATM[0] + Strike_price_diff
        strikePrice_ATM_down = strikePrice_ATM[0] - Strike_price_diff

        meta_trade_all_df['strikePrice_ATM_top'] = strikePrice_ATM_top
        meta_trade_all_df['strikePrice_ATM_down'] = strikePrice_ATM_down

        # filter data frame on EXP_date
        meta_trade_all_df = meta_trade_all_df[(meta_trade_all_df['CONTRACTS'] >= 100)]

        final_df = meta_trade_all_df[(meta_trade_all_df['strikePrice'] >= strikePrice_ATM_down) & (
                meta_trade_all_df['strikePrice'] <= strikePrice_ATM_top)]

        final_df = final_df[(final_df['CHG_IN_OI'] >= 300000) | (final_df['CHG_IN_OI'] <= -300000)]
        # final_df = final_df[final_df['VWAP'] * final_df['LOT'] > 3000]
        # df_final_all = pd.concat([df_final_all, final_df])
        df_final_all.append(final_df)
        #
        itm_opt_wrt_df = final_df[
            ((final_df['optionType'] == 'Call') & (final_df['strikePrice'] < final_df['Spot'])) | (
                    (final_df['optionType'] == 'Put') & (final_df['strikePrice'] > final_df['Spot']))]
        itm_opt_wrt_df['sp_spot_pc'] = np.where((itm_opt_wrt_df['optionType'] == 'Call'), (
                (itm_opt_wrt_df['Spot'] - itm_opt_wrt_df['strikePrice']) / itm_opt_wrt_df[
            'Spot']) * 100,
                                                ((itm_opt_wrt_df['strikePrice'] - itm_opt_wrt_df[
                                                    'Spot']) / itm_opt_wrt_df[
                                                     'Spot']) * 100)
        # df_itm_opt_wrt_all = pd.concat([df_itm_opt_wrt_all, itm_opt_wrt_df])
        df_itm_opt_wrt_all.append(itm_opt_wrt_df)


    except Exception as e:
        print(e)
        LOG_insert(LOG_FILE_NAME, str(e) + " Got error while updating master file", logging.ERROR)


get_data = pd.read_csv(METADATA_DIR + '//stockmetadata_nse_fut.csv')
# get_data = get_data.head(40)
get_data.sort_values(by=['SYMBOL'], ascending=True, inplace=True)
# uniqueValues = ['NIFTY', 'BANKNIFTY','APOLLOTYRE']
uniqueValues = get_data['SYMBOL'].unique()

s = requests.session()
url = "https://www.nseindia.com/"
s.get(url, headers=headers)
index = 0
for Symbol in uniqueValues:
    ic(Symbol)
    t1 = threading.Thread(target=start_thread, args=(Symbol, index, s))
    t1.start()
    index = index + 1
    threads.append(t1)
    if index % 30 == 0:
        sleep(1)
    if index % 200 == 0:
        s = requests.session()
        s.get('https://www.nseindia.com/get-quotes/derivatives?symbol=' + 'NIFTY', headers=headers)

for process in threads:
    process.join()
df_first_row_all = pd.concat(df_first_row_all)
all_stock_df = pd.concat(all_stock_df)
df_final_all = pd.concat(df_final_all)
df_itm_opt_wrt_all = pd.concat(df_itm_opt_wrt_all)


# writing to csv, rename & drop columns

if not os.path.exists(PROCESSED_DIR + '\\option' + '\\opt_chain_all_hist_' + sub_string_date + '.csv'):
    all_stock_df['expiryDate'] = all_stock_df['expiryDate'].dt.strftime('%d-%m-%Y')
    all_stock_df['date_today'] = all_stock_df['date_today'].dt.strftime('%d-%m-%Y %H:%M:%S')
    all_stock_df.sort_values(by=['STOCK'], ascending=True, inplace=True)
    all_stock_df.to_csv(PROCESSED_DIR + '\\option' + '\\opt_chain_all_hist_' + sub_string_date + '.csv', index=None)
else:
    historical = pd.read_csv(PROCESSED_DIR + '\\option' + '\\opt_chain_all_hist_' + sub_string_date + '.csv')
    historical['expiryDate'] = pd.to_datetime(historical['expiryDate'], format='%d-%m-%Y')
    historical['date_today'] = pd.to_datetime(historical['date_today'], format='%d-%m-%Y %H:%M:%S')
    if historical['date_today'].max() != all_stock_df['date_today'].max():
        historical = pd.concat([all_stock_df, historical])
        historical.sort_values(by=['STOCK'], ascending=True, inplace=True)
        historical['expiryDate'] = historical['expiryDate'].dt.strftime('%d-%m-%Y')
        historical['date_today'] = historical['date_today'].dt.strftime('%d-%m-%Y %H:%M:%S')
        historical.to_csv(PROCESSED_DIR + '\\option' + '\\opt_chain_all_hist_' + sub_string_date + '.csv', index=None)

if not os.path.exists(METADATA_DIR + '\\expiry_opt_metadata.csv'):
    df_first_row_all['expiryDate'] = df_first_row_all['expiryDate'].dt.strftime('%d-%m-%Y')
    df_first_row_all = df_first_row_all[['STOCK', 'expiryDate', 'marketLot', 'Strike_price_diff']]
    df_first_row_all.to_csv(METADATA_DIR + '\\expiry_opt_metadata.csv', index=None)
else:
    Historical_first_row = pd.read_csv(METADATA_DIR + '\\expiry_opt_metadata.csv')
    Historical_first_row['expiryDate'] = pd.to_datetime(Historical_first_row['expiryDate'], format='%d-%m-%Y')
    Historical_first_row = pd.concat([df_first_row_all, Historical_first_row])
    Historical_first_row = Historical_first_row.drop_duplicates(subset=['STOCK', 'expiryDate'], keep='first')
    Historical_first_row['expiryDate'] = Historical_first_row['expiryDate'].dt.strftime('%d-%m-%Y')
    Historical_first_row = Historical_first_row[['STOCK', 'expiryDate', 'marketLot', 'Strike_price_diff']]
    Historical_first_row.to_csv(METADATA_DIR + '\\expiry_opt_metadata.csv', index=None)

# To Remove timestamp
df_final_all['date_today'] = df_final_all['date_today'].dt.strftime('%d-%m-%Y')
df_final_all['date_today'] = pd.to_datetime(df_final_all['date_today'], format='%d-%m-%Y')
df_final_all = df_final_all.round({"pChange": 2})

EXP_date = EXP_date.strftime('%d-%m-%Y')
if not os.path.exists(PROCESSED_DIR + '\\option' + '\\opt_chain_filtred_3L_3KV_10pmatm_' + EXP_date +'.csv'):

    df_final_all['date_today'] = df_final_all['date_today'].dt.strftime('%d-%m-%Y')
    df_exp = df_final_all[df_final_all['expiryDate'] == index_expiry]
    df_exp = df_exp[(df_exp['STOCK'] == 'NIFTY') | (df_exp['STOCK'] == 'BANKNIFTY')| (df_exp['STOCK'] == 'FINNIFTY')]
    df_exp = df_exp[(df_exp['optionType'] == 'Call') | (df_exp['optionType'] == 'Put')]
    df_exp.sort_values(by=['STOCK', 'optionType', 'strikePrice'], ascending=True, inplace=True)
    df_exp.drop(['OPEN', 'HIGH', 'CLOSE', 'LTP', 'LOW', 'pChange', 'CONTRACTS', 'identifier', 'instrumentType', '%_of_OI',
                 'strikePrice_ATM_top', 'strikePrice_ATM_down'], inplace=True, axis=1)
    # df_exp['date_today'] = df_exp['date_today'].dt.strftime('%d-%m-%Y')
    df_exp.to_csv(RAW_DIR + '\\BN_Nifty_Major_opt_tab.csv', index=None)
    df_month_exp = df_final_all[df_final_all['expiryDate'] == stock_expiry]
    df_month_exp = df_month_exp[(df_month_exp['STOCK'] == 'NIFTY') | (df_month_exp['STOCK'] == 'BANKNIFTY')]
    df_month_exp = df_month_exp[(df_month_exp['optionType'] == 'Call') | (df_month_exp['optionType'] == 'Put')]
    df_month_exp.sort_values(by=['STOCK', 'optionType', 'strikePrice'], ascending=True, inplace=True)
    df_month_exp.drop(
        ['OPEN', 'HIGH', 'CLOSE', 'LTP', 'LOW', 'pChange', 'CONTRACTS', 'identifier', 'instrumentType', '%_of_OI',
         'strikePrice_ATM_top', 'strikePrice_ATM_down'], inplace=True, axis=1)
    df_month_exp.to_csv(RAW_DIR + '\\fut_BN_Nifty_Major_monthly_exp.csv', index=None)

    df_final_all['expiryDate'] = df_final_all['expiryDate'].dt.strftime('%d-%m-%Y')
    df_final_all.to_csv(PROCESSED_DIR + '\\option' + '\\opt_chain_filtred_3L_3KV_10pmatm_' + EXP_date + '.csv', index=None)
else:
    Historical_df_final_all = pd.read_csv(PROCESSED_DIR + '\\option' + '\\opt_chain_filtred_3L_3KV_10pmatm_' + EXP_date +'.csv')
    Historical_df_final_all['expiryDate'] = pd.to_datetime(Historical_df_final_all['expiryDate'], format='%d-%m-%Y')
    Historical_df_final_all['date_today'] = pd.to_datetime(Historical_df_final_all['date_today'], format='%d-%m-%Y')

    if Historical_df_final_all['date_today'].max() != df_final_all['date_today'].max():

        Historical_df_final_all = pd.concat([df_final_all, Historical_df_final_all])

        Historical_df_final_all['date_today'] = Historical_df_final_all['date_today'].dt.strftime('%d-%m-%Y')
        df_exp = Historical_df_final_all[Historical_df_final_all['expiryDate'] == index_expiry]
        df_exp = df_exp[(df_exp['STOCK'] == 'NIFTY') | (df_exp['STOCK'] == 'BANKNIFTY') | (df_exp['STOCK'] == 'FINNIFTY')]
        df_exp = df_exp[(df_exp['optionType'] == 'Call') | (df_exp['optionType'] == 'Put')]
        df_exp.sort_values(by=['STOCK', 'optionType', 'strikePrice'], ascending=True, inplace=True)
        df_exp.drop(['OPEN', 'HIGH', 'CLOSE', 'LTP', 'LOW', 'pChange', 'CONTRACTS', 'identifier', 'instrumentType', '%_of_OI',
                     'strikePrice_ATM_top', 'strikePrice_ATM_down'], inplace=True, axis=1)
        df_exp.to_csv(RAW_DIR + '\\BN_Nifty_Major_opt_tab.csv', index=None)
        df_month_exp = Historical_df_final_all[Historical_df_final_all['expiryDate'] == stock_expiry]
        df_month_exp = df_month_exp[(df_month_exp['STOCK'] == 'NIFTY') | (df_month_exp['STOCK'] == 'BANKNIFTY')
                                    | (df_month_exp['STOCK'] == 'FINNIFTY')]
        df_month_exp = df_month_exp[(df_month_exp['optionType'] == 'Call') | (df_month_exp['optionType'] == 'Put')]
        df_month_exp.sort_values(by=['STOCK', 'optionType', 'strikePrice'], ascending=True, inplace=True)
        df_month_exp.drop(
            ['OPEN', 'HIGH', 'CLOSE', 'LTP', 'LOW', 'pChange', 'CONTRACTS', 'identifier', 'instrumentType', '%_of_OI',
             'strikePrice_ATM_top', 'strikePrice_ATM_down'], inplace=True, axis=1)
        df_month_exp.to_csv(RAW_DIR + '\\fut_BN_Nifty_Major_monthly_exp.csv', index=None)

        Historical_df_final_all['expiryDate'] = Historical_df_final_all['expiryDate'].dt.strftime('%d-%m-%Y')
        Historical_df_final_all.to_csv(PROCESSED_DIR + '\\option' + '\\opt_chain_filtred_3L_3KV_10pmatm_' + EXP_date + '.csv', index=None)

df_itm_opt_wrt_all['expiryDate'] = df_itm_opt_wrt_all['expiryDate'].dt.strftime('%d-%m-%Y')
df_itm_opt_wrt_all['date_today'] = df_itm_opt_wrt_all['date_today'].dt.strftime('%d-%m-%Y')
df_itm_opt_wrt_all = df_itm_opt_wrt_all.round({"pChange": 2, "strike_spot_pc": 3})
df_itm_opt_wrt_all.to_csv(PROCESSED_DIR + '\\option' + '\\itm_opt_wrt' + sub_string_date + '.csv', index=None)
df_itm_opt_wrt_all.to_csv(RAW_DIR + '\\itm_opt_tab.csv', index=None)
#   write data into excel
# data_excel_file = SHARED_DIR + "\\fut_OI_historical.xlsm"
# wb_opt = xw.Book(data_excel_file)
# sheet_oi_single = wb_opt.sheets('BN_Nifty_Major_opt')
# sheet_oi_single.clear()
# sheet_oi_single.range("A2").options(index=None).value = df_exp
# sheet_oi_single = wb_opt.sheets('itm_opt')
# sheet_oi_single.clear()
# sheet_oi_single.range("A1").options(index=None).value = df_itm_opt_wrt_all
# wb_opt.save()
