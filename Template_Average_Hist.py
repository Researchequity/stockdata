import pandas as pd
from utils import *

if __name__ == '__main__':
    # avg_stock, avg_date, avg_col, Date_sorting
    bhav_data_nse_historical = pd.read_csv(PROCESSED_DIR + "\\bhav_data_nse_historical.csv")
    bhav_data_nse_historical['secWiseDelPosDate'] = pd.to_datetime(bhav_data_nse_historical['secWiseDelPosDate'],
                                                  format='%d-%m-%Y %H:%M', dayfirst=True)

    bhav_data_nse_historical['avg_stock'] = bhav_data_nse_historical['Stock']
    bhav_data_nse_historical['avg_date'] = bhav_data_nse_historical['secWiseDelPosDate']
    bhav_data_nse_historical['avg_col'] = bhav_data_nse_historical['quantityTraded']
    bhav_data_nse_historical.sort_values(by=['avg_date'], inplace=True)  # , ascending=False

    historical = 0
    if historical == 1:
        date = datetime.datetime.today()
        average_csv = average(bhav_data_nse_historical, date)
        average_csv.to_csv(METADATA_DIR + '//Average.csv', index=False)
    else:
        max_date = bhav_data_nse_historical['avg_date'].max()
        # min_date = bhav_data_nse_historical['avg_date'].min()
        min_date = bhav_data_nse_historical['avg_date'].max() - datetime.timedelta(days=1)
        totaldays = (max_date - min_date).days
        ic(totaldays)

        df_stock_all = pd.DataFrame()
        for i in range(totaldays + 1):
            date = min_date + datetime.timedelta(days=i)
            try:
                print(date)
                average_csv = average(bhav_data_nse_historical, date)
                df_stock_all = pd.concat([df_stock_all, average_csv])
            except:
                print('No data found for date')
                continue
        df_stock_all.to_csv(METADATA_DIR + '//Average_hist.csv', index=False)





