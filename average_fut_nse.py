from utils import *

file = os.path.basename(__file__)
date_today = datetime.date.today() - datetime.timedelta(days=1)
date_today, prev_date, fut_date = get_market_prev_date_fut_date(date_today)


def get_average(dataframe):
    uniqueValues = dataframe['SYMBOL'].unique()

    df_stock_all = pd.DataFrame()
    for stock in uniqueValues:
        print(stock)

        df_stock = dataframe[dataframe['SYMBOL'] == stock]

        df_stock.sort_values(by=['TIMESTAMP'], inplace=True)

        # slow mean via last 30 session
        df_stock = df_stock[0:30]

        df_CE = df_stock["VOL"].mean()
        df_CE = int(
            pd.DataFrame(np.where(df_stock['VOL'] < (2 * df_CE), df_stock['VOL'], np.nan)).mean())
        df_stock['Qty_mean_slow'] = round(df_CE)

        df_stock_perline_all = dataframe[dataframe['SYMBOL'] == stock]

        df_stock_perline_all.sort_values(by=['TIMESTAMP'], inplace=True)

        for row in range(len(df_stock_perline_all[30:])):
            df_stock_perline = df_stock_perline_all[row + 1:row + 30 + 1]
            df_CE = df_stock_perline["VOL"].mean()
            df_CE = int(pd.DataFrame(
                np.where(df_stock_perline['VOL'] < (2 * df_CE), df_stock_perline['VOL'],
                         np.nan)).mean())
            df_stock_perline.loc[df_stock_perline.index[-1], 'Qty_mean_slow'] = round(df_CE)
            df_stock = pd.concat([df_stock, df_stock_perline.tail(1)])

        # fast mean for last 8 session
        df_stock_perline_all = df_stock[df_stock['SYMBOL'] == stock]
        df_stock_perline_all.sort_values(by=['TIMESTAMP'], inplace=True)
        df_stock.sort_values(by=['TIMESTAMP'], inplace=True)
        df_stock = df_stock[0:8]

        # CE mean
        df_CE = df_stock["VOL"].mean()
        df_CE = int(
            pd.DataFrame(np.where(df_stock['VOL'] < (2 * df_CE), df_stock['VOL'], np.nan)).mean())
        df_stock['Qty_mean_fast'] = round(df_CE)

        for row in range(len(df_stock_perline_all[8:])):
            df_stock_perline = df_stock_perline_all[row + 1:row + 8 + 1]

            # CE mean
            df_CE = df_stock_perline["VOL"].mean()
            df_CE = int(pd.DataFrame(
                np.where(df_stock_perline['VOL'] < (2 * df_CE), df_stock_perline['VOL'],
                         np.nan)).mean())
            df_stock_perline.loc[df_stock_perline.index[-1], 'Qty_mean_fast'] = round(df_CE)
            df_stock = pd.concat([df_stock, df_stock_perline.tail(1)])

        df_stock_all = pd.concat([df_stock_all, df_stock])

    df_stock_all['TIMESTAMP'] = pd.to_datetime(df_stock_all['TIMESTAMP'], format='%d-%m-%Y')
    df_stock_all['TIMESTAMP'] = df_stock_all['TIMESTAMP'].dt.strftime('%d-%m-%Y')
    df_stock_all['vol_norm_mean'] = np.where(df_stock_all['Qty_mean_slow'] >= df_stock_all['Qty_mean_fast'],
                                             df_stock_all['Qty_mean_fast'], df_stock_all['Qty_mean_slow'])

    return df_stock_all


if __name__ == '__main__':
    try:
        exp_opt_metadata = pd.read_csv(METADATA_DIR + '\\expiry_opt_metadata.csv')[['STOCK', 'marketLot', 'expiryDate']]
        exp_opt_metadata['expiryDate'] = pd.to_datetime(exp_opt_metadata['expiryDate'], format='%d-%m-%Y')
        bhav_data_nse_fut = pd.read_csv(PROCESSED_DIR + '\\bhav_data_nse_fut_historical.csv')[['SYMBOL', 'CONTRACTS',
                                                                                               'TIMESTAMP', 'EXPIRY_DT']]
        bhav_data_nse_fut['TIMESTAMP'] = pd.to_datetime(bhav_data_nse_fut['TIMESTAMP'], format='%d-%m-%Y %H:%M')
        bhav_data_nse_fut['EXPIRY_DT'] = pd.to_datetime(bhav_data_nse_fut['EXPIRY_DT'], format='%d-%m-%Y %H:%M')
        exp_opt_metadata = exp_opt_metadata.rename(columns={"STOCK": "SYMBOL", "expiryDate": "EXPIRY_DT"})

        df_merge = pd.merge(bhav_data_nse_fut, exp_opt_metadata, on=['SYMBOL', 'EXPIRY_DT'], how='left')
        df_merge['VOL'] = df_merge['CONTRACTS'] * df_merge['marketLot']

        final_df = df_merge.groupby(['TIMESTAMP', 'SYMBOL']).agg(
            {'marketLot': ['sum'], 'CONTRACTS': ['first'], 'VOL': ['sum']}).reset_index()
        final_df.columns = ['TIMESTAMP', 'SYMBOL', 'marketLot_sum', 'CONTRACTS', 'VOL']

        final_df['VOL'].fillna(0, inplace=True)
        final_df = final_df[final_df.VOL != 0]

        day_30_df = final_df[final_df['SYMBOL'] == 'ADANIENT']
        day_30_df.sort_values(by=['TIMESTAMP'], ascending=False, inplace=True)  # desc
        day_30_df = day_30_df['TIMESTAMP'][:30].to_list()
        dataframe_30_days = pd.DataFrame()

        for dat in day_30_df:
            temp = final_df[final_df['TIMESTAMP'] == dat]
            dataframe_30_days = pd.concat([dataframe_30_days, temp])

        avg_df = get_average(dataframe_30_days)
        avg_df.to_csv(METADATA_DIR + '\\Average_fut_historical.csv', index=None)

        avg_df['TIMESTAMP'] = pd.to_datetime(avg_df['TIMESTAMP'], format='%d-%m-%Y')
        max_date = avg_df['TIMESTAMP'].unique().max()
        latest_df = avg_df[avg_df['TIMESTAMP'] == max_date]
        token_contract = pd.read_csv(METADATA_DIR + '\\token_contract.csv')
        df_merge = pd.merge(latest_df, token_contract, how='left', left_on='SYMBOL', right_on='Symbol')
        df_merge.drop(['marketLot_sum', 'VOL', 'Qty_mean_slow', 'Qty_mean_fast', 'Symbol', 'Option'], inplace=True, axis=1)
        df_merge.rename(columns={'SYMBOL': 'Stock'}, inplace=True)
        df_merge.to_csv(METADATA_DIR + '\\Average_fut.csv', index=None)

        execution_status('pass', file, '', date_today, 1)
    except Exception as e:
        print('err', str(e))
        execution_status(str(e), file, '', date_today, 0)
        sendmail_err(file, e)
        pass
