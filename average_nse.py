import pandas as pd
import numpy as np
from utils import *
# from filepath import BASE_DIR


pd.set_option('display.max_columns', None)


def get_average_data(filename,Average_CSV,norm_mean_CSV):
    try:
        delivery_position_historical_df = pd.read_csv(filename)
    except Exception as error:
        delivery_position_historical_df = pd.DataFrame()
        print("Creating File".format(error))

    delivery_position_historical_df['secWiseDelPosDate'] = pd.to_datetime(delivery_position_historical_df['secWiseDelPosDate'],
                                                                   format='%d-%m-%Y %H:%M', dayfirst=True)



    # Get last 30 days of data for average
    import datetime
    today = datetime.datetime.today()
    today = delivery_position_historical_df['secWiseDelPosDate'].max()
    lastmonth = today - datetime.timedelta(days=30)
    # Get last30days data

    delivery_position_historical_df_last30 = delivery_position_historical_df[
        delivery_position_historical_df['secWiseDelPosDate'] > lastmonth]

    # Get last 3days data for Count
    todaydate = datetime.date.today()
    todaydate = delivery_position_historical_df['secWiseDelPosDate'].max()
    if todaydate.weekday() in (3, 4):
        last3day = todaydate - datetime.timedelta(days=2)
    elif todaydate.weekday() in (0, 1, 2):
        last3day = todaydate - datetime.timedelta(days=4)
    else:
        last3day = todaydate - datetime.timedelta(days=3)

    last3day = pd.Timestamp(last3day)
    #delivery_position_historical_df_last3 = delivery_position_historical_df[delivery_position_historical_df['secWiseDelPosDate'] >= last3day]
    # delivery_position_groupby = delivery_position_historical_df_last30.groupby(['Stock'])
    # delivery_position_mean = delivery_position_groupby[['deliveryQuantity', 'lastPrice','quantityTraded','Trades']].mean()
    # delivery_position_mean.rename(columns={"deliveryQuantity": "mean_dQty", "lastPrice": "20DMA_price","quantityTraded":"mean_quantity","Trades":"mean_trd"}, inplace=True)

#get slow and fast mean
    uniqueValues = delivery_position_historical_df['Stock'].unique()

    df_stock = pd.DataFrame()

    for stock in uniqueValues:
        try:
            print(stock)

            df_stock_perline = delivery_position_historical_df[delivery_position_historical_df.Stock == stock]
            #df_stock_perline.sort_values(by=['secWiseDelPosDate'], inplace=True)

            # slow mean via last 30 session
            df_stock_perline = df_stock_perline[0:30]

            # qty mean
            df_mean = df_stock_perline["quantityTraded"].mean()
            df_mean = int(pd.DataFrame(np.where(df_stock_perline['quantityTraded'] < (2 * df_mean), df_stock_perline['quantityTraded'],
                               np.nan)).mean())
            df_stock_perline.loc[df_stock_perline.index[-1], 'vol_mean_slow'] = round(df_mean)

            # del mean
            df_mean_del = df_stock_perline["deliveryQuantity"].mean()
            df_mean_del = int(pd.DataFrame(np.where(df_stock_perline['deliveryQuantity'] < (2 * df_mean_del),
                                   df_stock_perline['deliveryQuantity'],np.nan)).mean())
            df_stock_perline.loc[df_stock_perline.index[-1], 'del_mean_slow'] = round(df_mean_del)
            # trade mean
            df_mean_trd = df_stock_perline["Trades"].mean()
            df_mean_trd = int(pd.DataFrame(np.where(df_stock_perline['Trades'] < (2 * df_mean_trd), df_stock_perline['Trades'], np.nan)).mean())
            df_stock_perline.loc[df_stock_perline.index[-1], 'trd_mean_slow'] = round(df_mean_trd)

            df_stock_perline = df_stock_perline[-8:]
            # qty mean
            df_mean = df_stock_perline["quantityTraded"].mean()
            df_mean = int(pd.DataFrame(np.where(df_stock_perline['quantityTraded'] < (2 * df_mean), df_stock_perline['quantityTraded'],np.nan)).mean())
            df_stock_perline.loc[df_stock_perline.index[-1], 'vol_mean_fast'] = round(df_mean)
            # del mean
            df_mean_del = df_stock_perline["deliveryQuantity"].mean()
            df_mean_del = int(pd.DataFrame(np.where(df_stock_perline['deliveryQuantity'] < (2 * df_mean_del),
                                   df_stock_perline['deliveryQuantity'],np.nan)).mean())
            df_stock_perline.loc[df_stock_perline.index[-1], 'del_mean_fast'] = round(df_mean_del)
            # trade mean
            df_mean_trd = df_stock_perline["Trades"].mean()
            df_mean_trd = int(pd.DataFrame(np.where(df_stock_perline['Trades'] < (2 * df_mean_trd), df_stock_perline['Trades'], np.nan)).mean())
            df_stock_perline.loc[df_stock_perline.index[-1], 'trd_mean_fast'] = round(df_mean_trd)

            df_stock = pd.concat([df_stock, df_stock_perline.tail(1)])
        except Exception as e:
            print(e)
            continue


    df_stock['secWiseDelPosDate'] = df_stock['secWiseDelPosDate'].dt.strftime('%d-%m-%Y %H:%M')
    df_stock['vol_norm_mean'] = np.where(df_stock['vol_mean_slow'] >= df_stock['vol_mean_fast'],
                                             df_stock['vol_mean_fast'], df_stock['vol_mean_slow'])
    df_stock['del_norm_mean'] = np.where(df_stock['del_mean_slow'] >= df_stock['del_mean_fast'],
                                             df_stock['del_mean_fast'], df_stock['del_mean_slow'])
    df_stock['trd_norm_mean'] = np.where(df_stock['trd_mean_slow'] >= df_stock['trd_mean_fast'],
                                             df_stock['trd_mean_fast'], df_stock['trd_mean_slow'])


    delivery_position_max = delivery_position_historical_df_last30.groupby(['Stock'])[['deliveryQuantity']].max().reset_index()
    delivery_position_max.columns = ['Stock','max_dQty']


    delivery_position_agg = pd.merge(df_stock, delivery_position_max, on=['Stock'], how= 'left')

    #delivery_position_agg = delivery_position_agg.round({"mean_dQty": 0}) #, "20DMA_price": 1

    delivery_position_agg = delivery_position_agg[['Stock','vol_norm_mean', 'del_norm_mean', 'trd_norm_mean','max_dQty']] #'quantityTraded','Trades','deliveryQuantity'
    delivery_position_agg['2time_20day'] = 0
    delivery_position_agg['3time_20day'] = 0
    delivery_position_agg['2time_3day'] = 0

    delivery_position_agg.to_csv(Average_CSV, index=False)

    # from datetime import datetime
    # date = datetime.now().strftime("%Y%m%d")
    # delivery_position_agg.to_csv(METADATA_DIR + "\\Average_{}.csv".format(date), index=False)


if __name__ == '__main__':
    # Getting path of CSV file
    Average_CSV = METADATA_DIR + '\\Average.csv'
    norm_mean_CSV = METADATA_DIR + '\\norm_mean.csv'

    bhav_data_nse_historical = PROCESSED_DIR + "\\bhav_data_nse_historical.csv"
    #bhav_data_bse_historical = "bhav_data_bse_historical.csv"
    get_average_data(bhav_data_nse_historical,Average_CSV,norm_mean_CSV)

