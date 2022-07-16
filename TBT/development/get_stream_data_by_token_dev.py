import warnings

warnings.simplefilter(action="ignore", category=Warning)

import getpass
import pandas as pd
import numpy as np
import os
from datetime import datetime

from get_stream_data_by_token_multi_token_filepath import *

date = datetime.today().strftime('%y-%m-%d')
pd.set_option('display.max_columns', None)


def add_missing_new_buy_and_sell_order(final_df_new, final_df_traded):
    df_buy_new_missing = final_df_traded[~final_df_traded['buyOrder'].isin(final_df_new['buyOrder'])]
    df_buy_new_missing['sellOrder'] = df_buy_new_missing['token']
    df_buy_new_missing[['token', 'orderType']] = ['B', 'N']
    df_sell_new_missing = final_df_traded[~final_df_traded['sellOrder'].isin(final_df_new['buyOrder'])]
    df_sell_new_missing['buyOrder'] = df_sell_new_missing['sellOrder']
    df_sell_new_missing['sellOrder'] = df_sell_new_missing['token']
    df_sell_new_missing[['token', 'orderType']] = ['S', 'N']
    return df_buy_new_missing, df_sell_new_missing


def add_missing_cancelled_order(final_df_cancelled, final_df_new):
    df_cancelled_new_missing = final_df_cancelled[~final_df_cancelled['buyOrder'].isin(final_df_new['buyOrder'])]
    df_cancelled_new_missing["orderType"].replace({"X": "N"}, inplace=True)
    return df_cancelled_new_missing


def duplicate_cancel_order(final_df_cancelled):
    # duplicate cancel order
    df_stream_token_cancel_groupby_buy = final_df_cancelled.groupby(['buyOrder'])
    df_stream_token_cancel_buy_count = df_stream_token_cancel_groupby_buy[['orderType']].count()
    df_stream_token_cancel_buy_count.rename(columns={"orderType": "count"}, inplace=True)
    df_stream_token_cancel_dup = df_stream_token_cancel_buy_count[df_stream_token_cancel_buy_count['count'] > 1]

    df_stream_token_cancel_dup = df_stream_token_cancel_dup.reset_index()

    cond = final_df_cancelled['buyOrder'].isin(df_stream_token_cancel_dup['buyOrder'])
    df_stream_token_cancelled_dups = final_df_cancelled[cond]  # Take cancel dups into seperate df
    final_df_cancelled.drop(final_df_cancelled[cond].index, inplace=True)
    # 1100000000444745
    return final_df_cancelled


def duplicate_new_order(df_new):
    # duplicate new order
    df_stream_token_new_groupby_buy = df_new.groupby(['buyOrder'])
    df_stream_token_new_buy_count = df_stream_token_new_groupby_buy[['orderType']].count()
    df_stream_token_new_groupby_buy = []
    df_stream_token_new_buy_count.rename(columns={"orderType": "count"}, inplace=True)
    df_stream_token_new_dup = df_stream_token_new_buy_count[df_stream_token_new_buy_count['count'] > 1]
    df_stream_token_new_dup = df_stream_token_new_dup.reset_index()

    cond = df_new['buyOrder'].isin(df_stream_token_new_dup['buyOrder'])
    df_new.drop(df_new[cond].index, inplace=True)

    return df_new


def main(interval, script_code, input_file_path, order_id):
    prefix = METADATA
    readrows_path = os.path.join(prefix, "readrows_{}_{}.csv".format(date, script_code))
    buy_file_path = os.path.join(prefix, "buy_{}_{}.csv".format(date, script_code))
    sell_file_path = os.path.join(prefix, "sell_{}_{}.csv".format(date, script_code))
    live_stream_dataframe = os.path.join(prefix, "live_stream_dataframe_{}_{}.csv".format(date, script_code))
    traded_round_time_data = os.path.join(prefix, "Traded_round_time_data_{}_{}.csv".format(date, script_code))
    final_df_path = os.path.join(prefix, "final_{}_{}.csv".format(date, script_code))

    read_row = 0 if not os.path.exists(readrows_path) else pd.read_csv(readrows_path)['rowcount'][0]
    print(input_file_path)
    df_stream_token_traded = pd.read_csv(input_file_path, header=None, chunksize=1000000, skiprows=read_row)

    final_df_traded = None

    final_df_new = None

    final_df_cancelled = None
    # import pdb;pdb.set_trace()
    for chunk in df_stream_token_traded:

        chunk.rename(
            columns={0: "uniqueRow", 1: "orderType", 2: "buyOrder", 3: "sellOrder", 4: "token", 5: "pricePaisa",
                     6: "quantity",
                     7: "datetime"}, inplace=True)

        chunk["datetime"] = pd.to_datetime(chunk['datetime'])
        chunk['buyOrder'] = np.where(chunk['buyOrder'] == 0, chunk['uniqueRow'], chunk['buyOrder'])  # check it
        chunk['sellOrder'] = np.where(chunk['sellOrder'] == 0, chunk['uniqueRow'], chunk['sellOrder'])
        chunk["uniqueRow"] = chunk["uniqueRow"].apply(str)
        chunk["buyOrder"] = chunk['buyOrder'].apply(str)
        chunk["sellOrder"] = chunk['sellOrder'].apply(str)

        df_traded = chunk[chunk['orderType'] == 'T']
        df_new = chunk[chunk['orderType'] == 'N']
        df_cancelled = chunk[chunk['orderType'] == 'X']

        if final_df_traded is None:
            final_df_traded = df_traded
        else:
            final_df_traded = pd.concat([final_df_traded, df_traded])

        if final_df_new is None:
            final_df_new = df_new
        else:
            final_df_new = pd.concat([final_df_new, df_new])

        if final_df_cancelled is None:
            final_df_cancelled = df_cancelled
        else:
            final_df_cancelled = pd.concat([final_df_cancelled, df_cancelled])

        read_row += len(chunk)
        # print(read_row,"read_rows")

    # there are orders where modification n price  cancel and create new order with same buyorder/sellorder
    final_df_new = duplicate_new_order(final_df_new)

    df_buy_new_missing, df_sell_new_missing = add_missing_new_buy_and_sell_order(final_df_new, final_df_traded)
    df_cancelled_new_missing = add_missing_cancelled_order(final_df_cancelled, final_df_new)
    df_missing = pd.concat([df_buy_new_missing, df_sell_new_missing])
    df_missing = pd.concat([df_missing, df_cancelled_new_missing])

    if df_missing.empty:
        print('')
    else:
        df_missing = df_missing.groupby(['buyOrder', 'sellOrder']).agg(
            {'uniqueRow': 'min', 'orderType': 'max', 'token': 'max', 'pricePaisa': 'mean', 'quantity': 'sum',
             'datetime': 'max'})
        df_missing = df_missing.reset_index(['sellOrder', 'buyOrder'])

    final_df_new = pd.concat([final_df_new, df_missing])

    df_missing = None
    df_cancelled_new_missing = None
    df_buy_new_missing = None
    df_sell_new_missing = None

    df_stream_token_traded_groupby_buy = final_df_traded.groupby(['buyOrder']).agg(
        {'quantity': sum, 'pricePaisa': ['mean'], 'datetime': ['max']})
    df_stream_token_traded_groupby_buy.columns = ["buyquantity", "b_avg_price", "b_datetime"]

    df_stream_token_traded_groupby_sell = final_df_traded.groupby(['sellOrder']).agg(
        {'quantity': sum, 'pricePaisa': ['mean'], 'datetime': ['max']})
    df_stream_token_traded_groupby_sell.columns = ["sellquantity", "s_avg_price", "s_datetime"]

    if not os.path.exists(buy_file_path):
        df_stream_token_traded_groupby_buy = df_stream_token_traded_groupby_buy.sort_values("buyquantity",
                                                                                            ascending=False)
        df_stream_token_traded_groupby_buy.index.name = 'buyOrder'
        df_stream_token_traded_groupby_buy.to_csv(buy_file_path)
    else:
        buy_csv = pd.read_csv(buy_file_path)
        buy_csv.set_index(['buyOrder'], inplace=True)
        df_stream_token_traded_groupby_buy = pd.concat([df_stream_token_traded_groupby_buy, buy_csv]).groupby(
            ['buyOrder']).agg({'buyquantity': ['sum'], 'b_avg_price': ['mean'], 'b_datetime': ['max']})
        df_stream_token_traded_groupby_buy.columns = ["buyquantity", "b_avg_price", "b_datetime"]
        df_stream_token_traded_groupby_buy = df_stream_token_traded_groupby_buy.sort_values("buyquantity",
                                                                                            ascending=False)
        df_stream_token_traded_groupby_buy.index.name = 'buyOrder'
        df_stream_token_traded_groupby_buy.to_csv(buy_file_path)

    if not os.path.exists(sell_file_path):
        df_stream_token_traded_groupby_sell = df_stream_token_traded_groupby_sell.sort_values("sellquantity",
                                                                                              ascending=False)
        df_stream_token_traded_groupby_sell.index.name = 'sellOrder'
        df_stream_token_traded_groupby_sell.to_csv(sell_file_path)
    else:
        sell_csv = pd.read_csv(sell_file_path)
        sell_csv.set_index(['sellOrder'], inplace=True)
        df_stream_token_traded_groupby_sell = pd.concat([df_stream_token_traded_groupby_sell, sell_csv]).groupby(
            ['sellOrder']).agg({'sellquantity': ['sum'], 's_avg_price': ['mean'], 's_datetime': ['max']})
        df_stream_token_traded_groupby_sell.columns = ["sellquantity", "s_avg_price", "s_datetime"]
        df_stream_token_traded_groupby_sell = df_stream_token_traded_groupby_sell.sort_values("sellquantity",
                                                                                              ascending=False)
        df_stream_token_traded_groupby_sell.index.name = 'sellOrder'
        df_stream_token_traded_groupby_sell.to_csv(sell_file_path)

    # Output Excel
    # sheet_oi_single = wb.sheets("big_quantity")
    # sheet_oi_single.range("A1").options().value = df_stream_token_traded_groupby_buy[df_stream_token_traded_groupby_buy['buyquantity'] > 1000].sort_values("buyquantity", ascending=False)
    # sheet_oi_single.range("E1").options().value = df_stream_token_traded_groupby_sell[df_stream_token_traded_groupby_sell['sellquantity'] > 1000].sort_values("sellquantity", ascending=False)

    df_stream_token_cancelled_new = final_df_cancelled[["quantity", "datetime", "buyOrder"]]

    df_stream_token_cancelled_new.rename(columns={"quantity": "cancellquantity", "datetime": "Cancelldatetime"},
                                         inplace=True)
    df_live_stream = final_df_new.merge(df_stream_token_cancelled_new, how='outer', on='buyOrder')

    if not os.path.exists(live_stream_dataframe):
        df_live_stream.to_csv(live_stream_dataframe, index=False)
    else:
        live_stream_old = pd.read_csv(live_stream_dataframe)
        live_stream_old['datetime'] = pd.to_datetime(live_stream_old['datetime'])
        # live_stream_old.columns =['uniqueRow','orderType','buyOrder','sellOrder','token','pricePaisa','quantity', 'datetime','cancellquantity','Cancelldatetime']
        df_live_stream = pd.concat([live_stream_old, df_live_stream])
        df_live_stream.to_csv(live_stream_dataframe, index=False)
    df_live_stream = df_live_stream.merge(df_stream_token_traded_groupby_buy, how='outer', on='buyOrder')

    # df_stream_token_traded_groupby_sell.reset_index(inplace=True)
    # df_stream_token_traded_groupby_sell.rename(columns={'sellOrder':'buyOrder'},inplace=True)
    df_stream_token_traded_groupby_sell.index.name = 'buyOrder'
    df_live_stream = df_live_stream.merge(df_stream_token_traded_groupby_sell, how='outer', on='buyOrder')

    df_live_stream['Round_Time'] = df_live_stream['datetime'].dt.floor(str(interval) + "min")
    # df_live_stream['Round_Time'] = pd.to_datetime(df_live_stream['Round_Time1']).dt.time
    # df_live_stream = df_live_stream.drop(columns='Round_Time1')

    final_df_traded['Round_Time'] = final_df_traded['datetime'].dt.floor(str(interval) + "min")
    # df_stream_token_traded['Round_Time'] = pd.to_datetime(df_stream_token_traded['Round_Time1']).dt.time
    # df_stream_token_traded = df_stream_token_traded.drop(columns='Round_Time1')

    ###round off pricePaisa column
    # take final_df_traded last row price

    last_price = final_df_traded['pricePaisa'].values[-1]

    # Price round logic =IF(B14>1000000,-3,IF(B14>100000,-2,IF(B14>15000,-1,0)))

    rounding = -3 if last_price > 1000000 else -2 if 1000000 > last_price > 100000 else -1 if 100000 > last_price > 15000 else 0

    # df_final price column round wuith above rounding factor and add price round rounding

    df_live_stream['Price_Round'] = df_live_stream['pricePaisa'].round(rounding)  # rounding it to 100 place

    ###column isDisclosed in all_trades
    df_live_stream['IsDisclosed'] = np.where(((df_live_stream.buyquantity) > (df_live_stream.quantity)) | (
            (df_live_stream.sellquantity) > (df_live_stream.quantity)), np.where(df_live_stream.buyquantity.isnull(), (
            df_live_stream.sellquantity / df_live_stream.quantity), (
                                                                                         df_live_stream.buyquantity / df_live_stream.quantity)),
                                             0)

    ###column Status in all_trades

    df_live_stream['quantity'] = df_live_stream['quantity'].fillna(0)
    df_live_stream['buyquantity'] = df_live_stream['buyquantity'].fillna(0)
    df_live_stream['sellquantity'] = df_live_stream['sellquantity'].fillna(0)
    df_live_stream['cancellquantity'] = df_live_stream['cancellquantity'].fillna(0)
    df_live_stream['bs'] = (df_live_stream['buyquantity'] + df_live_stream['sellquantity'])

    df_live_stream['Status'] = np.where((df_live_stream.buyquantity) == (df_live_stream.sellquantity),
                                        np.where(df_live_stream.buyquantity == 0,
                                                 np.where(((df_live_stream.cancellquantity) > 0), 'Cancel', 'Pending'),
                                                 np.nan), 'Traded')

    ###column Substatus
    df_live_stream['SubStatus'] = np.where(
        (df_live_stream.cancellquantity != 0) & (df_live_stream.buyquantity != df_live_stream.sellquantity),
        'Partial Cancel', np.where(
            (df_live_stream.bs < df_live_stream.quantity) & (df_live_stream.buyquantity != df_live_stream.sellquantity),
            'Partial Pending', 0))

    df_live_stream = df_live_stream.drop(columns='bs')

    if os.path.exists(traded_round_time_data):
        final_df_traded = pd.concat([final_df_traded, pd.read_csv(traded_round_time_data)])
    # final_df_traded['datetime']=pd.to_datetime(final_df_traded['datetime'])
    final_df_traded['Round_Time'] = pd.to_datetime(final_df_traded['Round_Time'])
    final_df_traded[final_df_traded['Round_Time'].isin([final_df_traded['Round_Time'].max()])].to_csv(
        traded_round_time_data, index=False)
    final_df_traded = final_df_traded[~final_df_traded['Round_Time'].isin([final_df_traded['Round_Time'].max()])]
    df1 = final_df_traded.groupby('Round_Time').agg(
        {'orderType': ['count'], 'pricePaisa': ['max'], 'quantity': ['sum']})

    # print(df1.1head())
    df_sell = df_live_stream[df_live_stream['token'] == 'S']
    df_buy = df_live_stream[df_live_stream['token'] == 'B']

    # print(df_buy.head(3))
    df_buy1 = df_buy.groupby('Round_Time').agg({'buyOrder': ['count'], 'quantity': ['sum']})
    df_sell1 = df_sell.groupby('Round_Time').agg({'sellOrder': ['count'], 'quantity': ['sum']})
    df2 = pd.merge(df_buy1, df_sell1, on="Round_Time", how='inner')

    df2.rename(columns={'quantity_x': 'sum of buyquantity', 'quantity_y': 'sum of sellquantity'}, inplace=True)
    # print(df2.head())

    df_final = pd.merge(df1, df2, on="Round_Time", how='inner')
    df_final.reset_index(inplace=True)
    df_final.columns = ['Round_Time', 'orderType', 'pricePaisa', 'quantity', 'buyOrder', 'sum of buyquantity',
                        'sellOrder', 'sum of sellquantity12']
    if os.path.exists(final_df_path):
        df_final = pd.concat([pd.read_csv(final_df_path), df_final])
        df_final.to_csv(final_df_path, index=False)
    else:
        df_final.to_csv(final_df_path, index=False)

    ### errorcode
    # import pdb;pdb.set_trace()
    df_live_stream['buyOrder1'] = df_live_stream['buyOrder'].astype(float)
    df_live_stream_error = df_live_stream[df_live_stream['buyOrder1'].isin(order_id)]
    df_live_stream_error = df_live_stream_error.drop(columns='buyOrder1')

    df_live_stream['uniqueRow'] = pd.to_numeric(df_live_stream['uniqueRow'])
    df_live_stream.sort_values(by=['uniqueRow'], inplace=True)

    df_live_stream = df_live_stream.drop(columns='buyOrder1')
    df_live_stream = df_live_stream.drop(columns='uniqueRow')
    final_df_traded = final_df_traded.drop(columns='uniqueRow')
    df_live_stream_error = df_live_stream_error.drop(columns='uniqueRow')

    df_live_stream_pending = df_live_stream[df_live_stream['Status'] == 'Pending']

    df_live_stream_pending = df_live_stream_pending.groupby(['Price_Round', 'token', 'sellOrder']).agg(
        {'quantity': sum}).reset_index()

    df_live_stream_pending.rename(columns={'quantity': 'new_quantity'}, inplace=True)

    df_live_stream_pending.sort_values(by=['Price_Round'], inplace=True, ascending=False)

    df_live_stream_pending = df_live_stream_pending.iloc[3:-3]

    df_live_stream_pending['token'] = 'Qty'

    df_live_stream_pending_gtr = df_live_stream_pending[df_live_stream_pending['Price_Round'] > last_price]

    df_live_stream_pending_smlr = df_live_stream_pending[df_live_stream_pending['Price_Round'] <= last_price]

    df_live_stream_pending_gtr.sort_values(by=['new_quantity'], inplace=True, ascending=False)

    df_live_stream_pending_smlr.sort_values(by=['new_quantity'], inplace=True, ascending=False)

    df_live_stream_pending_gtr = df_live_stream_pending_gtr.iloc[:3]
    df_live_stream_pending_smlr = df_live_stream_pending_smlr.iloc[:3]

    df_live_stream_pending_gtr['price_quantity'] = df_live_stream_pending_gtr['Price_Round'].astype(str) + '_' + \
                                                   df_live_stream_pending_gtr['new_quantity'].astype(str)

    df_live_stream_pending_smlr['price_quantity'] = df_live_stream_pending_smlr['Price_Round'].astype(str) + '_' + \
                                                    df_live_stream_pending_smlr['new_quantity'].astype(str)
    print(df_live_stream_pending_gtr[['Price_Round', 'new_quantity']].sort_values(by=['Price_Round']))

    print("Pivot is ", last_price)

    print(df_live_stream_pending_smlr[['Price_Round', 'new_quantity']].sort_values(by=['Price_Round'], ascending=False))

    df_live_stream_pending_gtr = df_live_stream_pending_gtr.pivot(index=['sellOrder'],
                                                                  columns=['price_quantity'],
                                                                  values=['Price_Round', 'new_quantity']).reset_index()

    df_live_stream_pending_gtr['sellOrder'] = df_live_stream_pending_gtr['sellOrder'].astype(int)
    df_live_stream_pending_gtr.columns = ['sellOrder', 'R1', 'R2', 'R3', 'Q1', 'Q2', 'Q3']

    if not os.path.exists(r'/home/workspace/aggregate/report/dsr_resistance.csv'):
        df_live_stream_pending_gtr.to_csv(r'/home/workspace/aggregate/report/dsr_resistance.csv', index=None)
    else:
        df_live_stream_pending_gtr.to_csv(r'/home/workspace/aggregate/report/dsr_resistance.csv', index=None,
                                          header=None, mode='a')

    df_live_stream_pending_smlr = df_live_stream_pending_smlr.pivot(index=['sellOrder'],
                                                                    columns=['price_quantity'], values=['Price_Round',
                                                                                                        'new_quantity']).reset_index()
    df_live_stream_pending_smlr['sellOrder'] = df_live_stream_pending_smlr['sellOrder'].astype(int)
    df_live_stream_pending_smlr.columns = ['sellOrder', 'S3', 'S2', 'S1', 'Q1', 'Q2', 'Q3']
    if not os.path.exists(r'/home/workspace/aggregate/report/dsr_support.csv'):
        df_live_stream_pending_smlr.to_csv(r'/home/workspace/aggregate/report/dsr_support.csv', index=None)
    else:
        df_live_stream_pending_smlr.to_csv(r'/home/workspace/aggregate/report/dsr_support.csv', index=None, header=None,
                                           mode='a')

    df_live_stream_pending_smlr['pivot'] = last_price

    # df_live_stream_pending = pd.merge(df_live_stream_pending_smlr, df_live_stream_pending_gtr, on='token')

    ##### saving the outputs to excel sheet
    pd.DataFrame([{'rowcount': read_row}]).to_csv(readrows_path, index=False)


def call_dsr(df):
    date = ''.join(str(datetime.today().date()).split('-'))
    timesleep = datetime.now().strftime('%H:%M')
    if timesleep < "09:45":
        if os.path.exists(r'/home/workspace/aggregate/report/dsr_support.csv'):
            os.remove(r'/home/workspace/aggregate/report/dsr_support.csv')
        if os.path.exists(r'/home/workspace/aggregate/report/dsr_resistance.csv'):
            os.remove(r'/home/workspace/aggregate/report/dsr_resistance.csv')

        tokens = df['token'].to_numpy()
        df['symbolInitial'] = df['Symbol'].str[:1]

        symbol_initial = df['symbolInitial'].to_numpy()

        streams = []
        for s in symbol_initial:
            if s >= 'A' and s <= 'D':
                s = 1
                streams.append(s)
            elif s >= 'E' and s <= 'I':
                s = 2
                streams.append(s)
            elif s >= 'J' and s <= 'Q':
                s = 3
                streams.append(s)
            elif s >= 'R' and s <= 'Z':
                s = 4
                streams.append(s)
            else:
                s = 1
                streams.append(s)

        print(streams, 'stream')
        print(tokens, 'tokens')

        for token, stream in zip(tokens, streams):
            minround = 1


            token = str(token)  # .astype(str)
            # stream = "4"

            stream_path = DUMPER_FILE_DIRECTORY + "DUMP_" + str(date) + "_07300" + str(stream) + ".csv_Clean"
            drop_path = AGGREGATE_PATH + "stream_by_token_" + str(date) + "_" + token + ".csv"

            os.system(("grep {} {} > {}").format("," + token + ",", stream_path, drop_path + "_str"))

            min_value = int(0)
            order_id = []
            with open(drop_path, 'w') as fd:
                with open(drop_path + "_str", 'r') as read_obj:
                    for line in read_obj:
                        first = line.split(',')
                        if (first[1] == "N"):
                            if (int(first[2]) >= int(min_value)):
                                min_value = first[2]
                            else:
                                order_id.append(first[2])
                        if ((first[3] == token) or (first[4] == token)):
                            fd.write(line)

            # subprocess.call("del {}".format(drop_path + "_str"), shell=True)
            # try:
            main(minround, token, drop_path, order_id)



if __name__ == '__main__':
    warnings.simplefilter(action='ignore', category=Warning)

    read_row = 0
    # df = pd.read_csv(r'/home/workspace/aggregate/report/trade_watch_merge_Report_Buy.csv')[['token', 'Symbol']]
    # df = df.head(1)

    call_dsr(df)
