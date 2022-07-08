import pandas as pd
import requests
import os.path
import urllib
import socket
import mimetypes
import xlwings as xw
import win32com.client
import warnings

warnings.simplefilter(action="ignore", category=Warning)
import time
from urllib.request import urlopen
from email.utils import make_msgid
from utils import *

socket.setdefaulttimeout(5)
file = os.path.basename(__file__)

LOG_FILE_NAME = str(LOG_DIR) + "\\get_BHAV_FILE_NSE_FUT_Historical.log"

global fii_stats_date, str_date
date_today = datetime.date.today() #- datetime.timedelta(days=1)
date, prev_date, fut_date = get_market_prev_date_fut_date(date_today)

prev_date = prev_date.strftime('%d%m%Y')
fii_stats_date = date.strftime('%d-%b-%Y')
str_date = date.strftime('%d%m%Y')
mon_str = date.strftime('%b').upper()
year_str = str_date[-4:]
day_str = str_date[:2]


def get_nse_Bhav_file(provided_date=None):
    if provided_date:
        str_date = provided_date

    BHAV_FILE_NAME = f'fo{day_str}{mon_str}{year_str}bhav.csv'
    print(BHAV_FILE_NAME)

    if not os.path.exists(BASE_DIR + '\\raw\\' + BHAV_FILE_NAME):
        # downloading bhav file https://archives.nseindia.com/content/historical/DERIVATIVES/2021/NOV/fo04MAR2022bhav.csv.zip
        try:
            response = requests.get(
                f"https://archives.nseindia.com/content/historical/DERIVATIVES/{year_str}/{mon_str}/fo{day_str}{mon_str}{year_str}bhav.csv.zip",
                timeout=10)
            open(BASE_DIR + '\\raw\\' + BHAV_FILE_NAME + '.zip', 'wb').write(response.content)
            return BASE_DIR + '\\raw\\' + BHAV_FILE_NAME + '.zip'

        except Exception as e:
            print(e)
            return None


def filter_data(bhav_copy):
    return bhav_copy[bhav_copy['INSTRUMENT'].isin(['FUTIDX', 'FUTSTK'])]
    """
    ['INSTRUMENT' 'SYMBOL' 'EXPIRY_DT' 'STRIKE_PR' 'OPTION_TYP' 'OPEN' 'HIGH'
     'LOW' 'CLOSE' 'SETTLE_PR' 'CONTRACTS' 'VAL_INLAKH' 'OPEN_INT' 'CHG_IN_OI'
     'TIMESTAMP' 'Unnamed: 15']
    """


def update_nse_fut_master_file(bhav_file_path, str_date):
    bhav_data_nse_master_file = "bhav_data_nse_fut_historical.csv"

    bhav_copy = pd.read_csv(bhav_file_path)

    bhav_copy = filter_data(bhav_copy)

    bhav_copy['EXPIRY_DT'] = pd.to_datetime(bhav_copy['EXPIRY_DT'], format='%d-%b-%Y')
    bhav_copy['TIMESTAMP'] = pd.to_datetime(bhav_copy['TIMESTAMP'], format='%d-%b-%Y')
    bhav_copy.sort_values(by=['EXPIRY_DT'], inplace=True, ascending=True)
    bhav_copy['EXPIRY_DT'] = bhav_copy['EXPIRY_DT'].dt.strftime('%d-%m-%Y %H:%M')  # .apply(date_correction)
    bhav_copy['TIMESTAMP'] = bhav_copy['TIMESTAMP'].dt.strftime('%d-%m-%Y %H:%M')  # .apply(date_correction)
    bhav_copy['pChange'] = ((bhav_copy['CLOSE'] - bhav_copy['OPEN']) / bhav_copy['OPEN']) * 100
    bhav_copy['pChange'] = bhav_copy['pChange'].round(1)

    bhav_copy_stockmetadata_fut = bhav_copy[['INSTRUMENT', 'SYMBOL', 'EXPIRY_DT']]
    bhav_copy_stockmetadata_fut.to_csv(METADATA_DIR + "\\stockmetadata_nse_fut.csv", index=False)

    bhav_copy.drop(
        ['STRIKE_PR', 'OPTION_TYP', 'Unnamed: 15'],
        axis=1, inplace=True)

    try:
        if not os.path.exists(PROCESSED_DIR + '\\' + bhav_data_nse_master_file):
            bhav_copy.to_csv(PROCESSED_DIR + '\\' + bhav_data_nse_master_file, index=False)
        else:
            bhav_data_nse_master_file_dataframe = pd.read_csv(PROCESSED_DIR + '\\' + bhav_data_nse_master_file)
            if not (datetime.datetime.today().date() == pd.to_datetime(
                    bhav_data_nse_master_file_dataframe['TIMESTAMP']).max().date()) and \
                    pd.to_datetime(bhav_data_nse_master_file_dataframe['TIMESTAMP'], format='%d-%m-%Y %H:%M').max() \
                    != pd.to_datetime(bhav_copy['TIMESTAMP'], format='%d-%m-%Y %H:%M').max():
                pd.concat([bhav_copy, bhav_data_nse_master_file_dataframe]).to_csv(
                    PROCESSED_DIR + '\\' + bhav_data_nse_master_file, index=False)

    except Exception as e:
        print(LOG_FILE_NAME)
        LOG_insert(LOG_FILE_NAME, str(e) + " Got error while updating master file", logging.ERROR)

    deleting_csv_files(bhav_file_path.split('.zip')[0], LOG_FILE_NAME)
    deleting_zipped_folder(LOG_FILE_NAME)
    return


def filter_data_opt(bhav_copy):
    return bhav_copy[bhav_copy['INSTRUMENT'].isin(['OPTSTK', 'OPTIDX'])]


def update_nse_opt_master_file(bhav_file_path, str_date):
    bhav_data_nse_master_file = "bhav_data_nse_opt_" + str_date + ".csv"
    bhav_copy = pd.read_csv(bhav_file_path)
    bhav_copy = filter_data_opt(bhav_copy)
    bhav_copy['EXPIRY_DT'] = pd.to_datetime(bhav_copy['EXPIRY_DT'], format='%d-%b-%Y')
    bhav_copy['TIMESTAMP'] = pd.to_datetime(bhav_copy['TIMESTAMP'], format='%d-%b-%Y')
    bhav_copy.sort_values(by=['EXPIRY_DT'], inplace=True, ascending=True)
    bhav_copy['pChange'] = ((bhav_copy['CLOSE'] - bhav_copy['OPEN']) / bhav_copy['OPEN']) * 100
    bhav_copy['pChange'] = bhav_copy['pChange'].round(1)
    bhav_copy_stockmetadata_fut = bhav_copy[['INSTRUMENT', 'SYMBOL', 'EXPIRY_DT']]
    bhav_copy_stockmetadata_fut['EXPIRY_DT'] = bhav_copy_stockmetadata_fut['EXPIRY_DT'].dt.strftime('%d-%m-%Y')
    bhav_copy_stockmetadata_fut.to_csv(METADATA_DIR + "\\stockmetadata_nse_opt_CE.csv", index=False)
    bhav_copy.drop(['Unnamed: 15'], axis=1, inplace=True)  # 'EXPIRY_DT',

    try:
        if not os.path.exists(PROCESSED_DIR + '\\option' + '\\' + bhav_data_nse_master_file):
            bhav_copy['EXPIRY_DT'] = bhav_copy['EXPIRY_DT'].dt.strftime('%d-%m-%Y')  # .apply(date_correction)
            bhav_copy['TIMESTAMP'] = bhav_copy['TIMESTAMP'].dt.strftime('%d-%m-%Y')  # .apply(date_correction)
            bhav_copy.to_csv(PROCESSED_DIR + '\\option' + '\\' + bhav_data_nse_master_file, index=False)
        else:
            bhav_data_nse_master_file_dataframe = pd.read_csv(
                PROCESSED_DIR + '\\option' + '\\' + bhav_data_nse_master_file)
            bhav_data_nse_master_file_dataframe['TIMESTAMP'] = pd.to_datetime(
                bhav_data_nse_master_file_dataframe['TIMESTAMP'], format='%d-%m-%Y')
            bhav_data_nse_master_file_dataframe['EXPIRY_DT'] = pd.to_datetime(
                bhav_data_nse_master_file_dataframe['EXPIRY_DT'], format='%d-%m-%Y')
            if not (datetime.datetime.today().date() == pd.to_datetime(
                    bhav_data_nse_master_file_dataframe['TIMESTAMP']).max().date()):
                bhav_copy = pd.concat([bhav_copy, bhav_data_nse_master_file_dataframe])
                bhav_copy['EXPIRY_DT'] = bhav_copy['EXPIRY_DT'].dt.strftime('%d-%m-%Y')
                bhav_copy['TIMESTAMP'] = bhav_copy['TIMESTAMP'].dt.strftime('%d-%m-%Y')
                bhav_copy.to_csv(PROCESSED_DIR + '\\option' + '\\' + bhav_data_nse_master_file, index=False)

    except Exception as e:
        print(LOG_FILE_NAME)
        LOG_insert(LOG_FILE_NAME, str(e) + " Got error while updating master file", logging.ERROR)

    return


def get_historical_bhav_file():
    try:
        bhav_file_path = get_nse_Bhav_file()
        # bhav_file_path = r'\\192.168.41.190\program\stockdata\raw\fo03MAR2022bhav.csv.zip'

        unzip_folder(bhav_file_path, LOG_FILE_NAME)

        if bhav_file_path:
            print(bhav_file_path, 'bhav file path')
            update_nse_opt_master_file(bhav_file_path.split('.zip')[0], str_date)
            update_nse_fut_master_file(bhav_file_path.split('.zip')[0], str_date)
    except Exception as e:
        LOG_insert(LOG_FILE_NAME, str(e) + "File Not Available On This", logging.ERROR)


def mwpl_cli_report():
    str_date = prev_date
    print(str_date)
    try:
        dls = "https://archives.nseindia.com/content/nsccl/mwpl_cli_{}.xls".format(str_date)
        urllib.request.urlretrieve(dls, RAW_DIR + "\\mwpl_cli_{}.xls".format(str_date))
    except:
        print('mwpl_cli file not found for date' + str_date)

    result = pd.read_csv(PROCESSED_DIR + '\\mwpl_cli_report_historical.csv')
    result['date'] = pd.to_datetime(result.date, format='%d-%m-%Y')

    report_frame = pd.read_excel(RAW_DIR + '\\mwpl_cli_{}.xls'.format(str_date), skiprows=1)
    report_frame.drop(['Sr No.'], axis=1, inplace=True)
    df_sum = report_frame.sum(axis=1)
    df_mean = report_frame.mean(axis=1)
    df_count = report_frame.loc[:, 'Client 1':].count(axis=1)
    report_frame = pd.concat([report_frame, df_sum, df_mean, df_count], axis=1)
    report_frame['date'] = pd.to_datetime(str_date, format='%d%m%Y')
    report_frame.fillna(0, inplace=True)

    # rename columns
    report_frame.rename(columns={0: 'SUM'}, inplace=True)
    report_frame.rename(columns={1: 'AVG'}, inplace=True)
    report_frame.rename(columns={2: 'COUNT'}, inplace=True)
    report_frame = report_frame[['Underlying Stock', 'SUM', 'AVG', 'COUNT', 'date']]
    report_frame = pd.DataFrame(report_frame)

    if report_frame.date.max() != result.date.max():
        result = pd.concat([report_frame, result])
        # result = pd.DataFrame(result)
        result['date'] = result['date'].dt.strftime('%d-%m-%Y')
        result.to_csv(PROCESSED_DIR + '\\mwpl_cli_report_historical.csv', index=None)

    result['date'] = pd.to_datetime(result.date, format='%d-%m-%Y')

    # Today Yesterday analysis
    date_unique = result['date'].unique()

    unique_date_frame = pd.DataFrame(date_unique)
    unique_date_frame[0] = pd.to_datetime(unique_date_frame[0], format='%d-%m-%Y')

    unique_date_frame.sort_values(by=0, ascending=False, inplace=True)
    Today_date = unique_date_frame[0].iloc[0]
    yesterday_date = unique_date_frame[0].iloc[1]
    today_df = result.loc[result['date'] == Today_date]

    yesterday_df = result.loc[result['date'] == yesterday_date]
    last_date = unique_date_frame[0].iloc[9]  # for last 10 days data date

    df_output = pd.merge(today_df, yesterday_df, on='Underlying Stock', how='left').round(decimals=2)
    df_output.columns = ['Stock', 'SUM_Today', 'AVG_Today', 'Count_Today', 'Date_today', 'SUM_Yesterday',
                         'AVG_Yesterday', 'Count_Yesterday', 'date_yesterday']
    df_output = df_output[
        ['Stock', 'SUM_Today', 'SUM_Yesterday', 'Count_Today', 'Count_Yesterday',
         'Date_today', 'date_yesterday']]

    df_output = df_output[(df_output['SUM_Today'] != df_output['SUM_Yesterday']) | (
            df_output['Count_Today'] != df_output['Count_Yesterday'])]
    df_output.sort_values(by='Count_Today', ascending=False, inplace=True)

    # Historical analysis
    last_date = pd.to_datetime(last_date)
    result = result.loc[result['date'] >= last_date]
    # result = result[result['date'] >= last_date]

    result['count_sum'] = result['COUNT'].astype(str) + " - " + round(result['SUM'], 1).astype(str)
    new_df = result[['Underlying Stock', 'date', 'count_sum']]
    new_df.set_index(['date', 'Underlying Stock'], inplace=True)
    new_df = new_df.unstack([0])
    new_df = pd.DataFrame(new_df)

    new_df.sort_values(by=['date'], ascending=False, inplace=True, axis=1)
    os.remove(RAW_DIR + '\\mwpl_cli_{}.xls'.format(str_date))

    df_output.to_csv(RAW_DIR+'//fut_Today_yesterday.csv', index=None)
    new_df.to_csv(RAW_DIR + '//fut_Historical_data.csv')


def get_opt_data():
    df = pd.read_csv(PROCESSED_DIR + '\\option' + '\\bhav_data_nse_opt_' + str_date + '.csv')
    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], format='%d-%m-%Y')
    df_today = df.groupby(['TIMESTAMP', 'SYMBOL', 'OPTION_TYP']).agg(
        {'OPEN_INT': ['sum'], 'CHG_IN_OI': ['sum']}).reset_index()
    df_today.columns = ['TIMESTAMP', 'SYMBOL', 'OPTION_TYP', 'OPEN_INT_sum', 'CHG_IN_OI_sum']
    df_today = pd.DataFrame(df_today)

    df_today = pd.pivot_table(df_today, values=['OPEN_INT_sum', 'CHG_IN_OI_sum'], index=['TIMESTAMP', 'SYMBOL'],
                              columns=['OPTION_TYP']).reset_index()
    df_today.columns = ['TIMESTAMP', 'SYMBOL', 'CHG_IN_OI_sum_CE', 'CHG_IN_OI_sum_PE', 'OPEN_INT_sum_CE',
                        'OPEN_INT_sum_PE']

    if not os.path.exists(PROCESSED_DIR + '\\get_opt_data_historical.csv'):
        df_today['TIMESTAMP'] = df_today['TIMESTAMP'].dt.strftime('%d-%m-%Y')
        df_today.to_csv(PROCESSED_DIR + '\\get_opt_data_historical.csv', index=None)
    else:
        Historical = pd.read_csv(PROCESSED_DIR + '\\get_opt_data_historical.csv')
        Historical['TIMESTAMP'] = pd.to_datetime(Historical.TIMESTAMP, format='%d-%m-%Y')
        if Historical['TIMESTAMP'].max() != df_today['TIMESTAMP'].max():
            Historical = pd.concat([df_today, Historical])
            Historical['TIMESTAMP'] = Historical['TIMESTAMP'].dt.strftime('%d-%m-%Y')
            Historical.to_csv(PROCESSED_DIR + '\\get_opt_data_historical.csv', index=None)

    return df_today


def load_fut_dashboard():
    # Read historical file
    df = pd.read_csv(PROCESSED_DIR + '\\bhav_data_nse_fut_historical.csv')
    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], format='%d-%m-%Y %H:%M')
    df['EXPIRY_DT'] = pd.to_datetime(df['EXPIRY_DT'], format='%d-%m-%Y %H:%M')

    df_expiry = df[(df['TIMESTAMP'] == df['EXPIRY_DT'])]
    if not df_expiry.empty:
        exp_dates = df_expiry['EXPIRY_DT'].unique()

    df = df[df['TIMESTAMP'] != df['EXPIRY_DT']]
    df.sort_values(["TIMESTAMP", "SYMBOL", "EXPIRY_DT"], ascending=True, inplace=True)
    hist_data_grp_daily_df = df.groupby(['TIMESTAMP', 'SYMBOL']).agg(
        {'OPEN_INT': ['sum'], 'CLOSE': ['first'], 'CHG_IN_OI': ['sum']}).reset_index()
    hist_data_grp_daily_df.columns = ['TIMESTAMP', 'SYMBOL', 'OPEN_INT_sum', 'Close_first', 'CHG_IN_OI_sum']

    exp_df = pd.DataFrame()
    for max_date in exp_dates:
        exp_date_df = hist_data_grp_daily_df[hist_data_grp_daily_df['TIMESTAMP'] == max_date]
        prev_df = hist_data_grp_daily_df[hist_data_grp_daily_df['TIMESTAMP'] < max_date]
        yesterday_df = prev_df[prev_df['TIMESTAMP'] == prev_df['TIMESTAMP'].max()]
        yesterday_df = yesterday_df[['SYMBOL', 'OPEN_INT_sum']]
        yesterday_df = yesterday_df.rename(columns={"OPEN_INT_sum": "OPEN_INT_sum_yest"})
        df_merge = pd.merge(exp_date_df, yesterday_df, on=['SYMBOL'], how='left')
        df_merge['CHG_IN_OI_sum_new'] = df_merge['OPEN_INT_sum'] - df_merge['OPEN_INT_sum_yest']
        df_merge.drop(['OPEN_INT_sum_yest', 'CHG_IN_OI_sum'], inplace=True, axis=1)
        df_merge = df_merge.rename(columns={"CHG_IN_OI_sum_new": "CHG_IN_OI_sum"})
        exp_df = exp_df.append(df_merge)

    hist_data_grp_daily_df = hist_data_grp_daily_df[~hist_data_grp_daily_df['TIMESTAMP'].isin(exp_dates)]
    hist_data_grp_daily_df = hist_data_grp_daily_df.append(exp_df)
    hist_data_grp_daily_df_csv = hist_data_grp_daily_df
    hist_data_grp_daily_df_csv['TIMESTAMP'] = hist_data_grp_daily_df_csv['TIMESTAMP'].dt.strftime('%d-%m-%Y')
    hist_data_grp_daily_df_csv.to_csv(REPORT_DIR + '\\bhav_data_nse_fut_agg_oi.csv', index=None)
    hist_data_grp_daily_df_csv['TIMESTAMP'] = pd.to_datetime(hist_data_grp_daily_df_csv['TIMESTAMP'], format='%d-%m-%Y')
    filt_date = datetime.date.today() - datetime.timedelta(days=180)
    filt_date = pd.to_datetime(filt_date, format='%Y-%m-%d')
    hist_data_grp_daily_df_csv['%chng'] = ((hist_data_grp_daily_df_csv['CHG_IN_OI_sum'] / \
                                            hist_data_grp_daily_df_csv['OPEN_INT_sum']) * 100).round(2)
    hist_data_grp_daily_df_csv = hist_data_grp_daily_df_csv[hist_data_grp_daily_df_csv.TIMESTAMP >= filt_date]
    hist_data_grp_daily_df_csv.sort_values(by=['SYMBOL', 'TIMESTAMP'], ascending=False, inplace=True)

    df_today = pd.DataFrame(hist_data_grp_daily_df)
    df_today['TIMESTAMP'] = pd.to_datetime(df_today['TIMESTAMP'],
                                           format='%d-%m-%Y')

    date_unique = df['TIMESTAMP'].unique()
    unique_date_frame = pd.DataFrame(date_unique)
    last_week_date = unique_date_frame[0].iloc[-7]
    last_month_date = unique_date_frame[0].iloc[-26]
    yesterday_date = unique_date_frame[0].iloc[-1]

    global last_date
    last_date = unique_date_frame[0].iloc[-1]  # date to be used in sending EMail

    yesterday_df = df_today.loc[(df_today.TIMESTAMP == yesterday_date)]
    yesterday_df = pd.DataFrame(yesterday_df)

    last_Week_df = df_today.loc[(df_today.TIMESTAMP <= last_week_date) & (df_today.TIMESTAMP >= last_month_date)]
    last_Week_price_df = df_today.loc[(df_today.TIMESTAMP == last_week_date)]
    last_Week_price_df = last_Week_price_df[['SYMBOL', 'Close_first']]
    last_Week_df = last_Week_df.groupby(['SYMBOL']).agg({'OPEN_INT_sum': ['mean']}).reset_index()
    last_Week_df.columns = ['SYMBOL', 'OPEN_INT_Mean']
    last_Week_df = pd.merge(last_Week_df, last_Week_price_df, on=['SYMBOL'])
    last_Week_df = pd.DataFrame(last_Week_df)

    last_full_Week_df = df_today.loc[(df_today.TIMESTAMP >= last_week_date)]
    last_full_Week_df = pd.DataFrame(last_full_Week_df)
    # last_full_Week_df.to_csv(r'\\192.168.41.190\program\stockdata\raw\asasa.csv')
    get_filt_data = last_full_Week_df[(last_full_Week_df['SYMBOL'] == 'NIFTY') | (
            last_full_Week_df['SYMBOL'] == 'BANKNIFTY')]  # (last_full_Week_df['SYMBOL'] == 'FINNIFTY') |
    get_filt_data = pd.DataFrame(get_filt_data)

    min_max_df = hist_data_grp_daily_df.groupby(['SYMBOL']).agg({'OPEN_INT_sum': ['max', 'mean', 'min']}).reset_index()
    min_max_df.columns = ['SYMBOL', 'Max_OPEN_INT', 'Average_OPEN_INT', 'Min_OPEN_INT']
    min_max_df = pd.DataFrame(min_max_df)

    filt = df[(df['TIMESTAMP'] == yesterday_date) & (df['SYMBOL'] == 'NIFTY')]
    filt = pd.DataFrame(filt)
    Min_exp_date = filt['EXPIRY_DT'].iloc[0]
    min_expiry = df[(df['TIMESTAMP'] == yesterday_date) & (df['EXPIRY_DT'] == Min_exp_date)]
    min_expiry = pd.DataFrame(min_expiry)

    # get MWPL file
    response = requests.get(f"https://archives.nseindia.com/content/nsccl/nseoi.zip", timeout=1)
    open(RAW_DIR + '\\nseoi.zip', 'wb').write(response.content)
    zf = ZipFile(RAW_DIR + "\\nseoi.zip")
    zf.extractall(RAW_DIR)
    zf.close()
    name = pd.DataFrame(zf.namelist())
    csv_name = str(name.iloc[0])
    sub_string_csv = csv_name[11:19]

    path = str(RAW_DIR + '\\nseoi_' + sub_string_csv + '.csv')
    Mwpl_df = pd.read_csv(path)
    os.remove(RAW_DIR + "\\nseoi.zip")
    os.remove(RAW_DIR + "\\nseoi_" + sub_string_csv + ".csv")
    os.remove(RAW_DIR + "\\nseoi_" + sub_string_csv + ".xml")

    Mwpl_df['date'] = sub_string_csv
    Mwpl_df['date'] = pd.to_datetime(Mwpl_df['date'], format='%d%m%Y')
    Mwpl_df['date'] = Mwpl_df['date'].dt.strftime('%d-%m-%Y')
    Mwpl_df = Mwpl_df[[' NSE Symbol', ' MWPL', ' ISIN', ' Scrip Name', ' NSE Open Interest', 'date']]

    option_data_df = get_opt_data()
    option_data_df['TIMESTAMP'] = pd.to_datetime(option_data_df.TIMESTAMP)
    pd.set_option("display.max_rows", None, "display.max_columns", None)
    min_expiry = pd.merge(min_expiry, option_data_df, on=['SYMBOL', 'TIMESTAMP'], how='left')

    min_expiry.to_csv(RAW_DIR + '//fut_Report_data.csv', index=None)
    min_max_df.to_csv(RAW_DIR + '//fut_AvgMinMaxOI.csv', index=None)
    last_Week_df.to_csv(RAW_DIR + '//fut_Last_Week_OI.csv', index=None)
    yesterday_df.to_csv(RAW_DIR + '//fut_Today_OI.csv', index=None)
    hist_data_grp_daily_df_csv.to_csv(RAW_DIR + '//fut_DailyAgg_OI.csv', index=None)
    Mwpl_df.to_csv(RAW_DIR + '//fut_MWPL.csv', index=None)
    get_filt_data.sort_values(['SYMBOL', 'TIMESTAMP']).to_csv(RAW_DIR + '//fut_Index.csv', index=None)

    hist_data_grp_daily_df_csv.to_csv(RAW_DIR + '//fut_DailyAgg_OI.csv', index=None)
    hist_data_grp_daily_df_csv.to_csv(RAW_DIR + '//fut_DailyAgg_OI.csv', index=None)


def change_oi_monthly():
    df = pd.read_csv(r'\\192.168.41.190\report\bhav_data_nse_fut_agg_oi.csv')

    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], format='%d-%m-%Y')
    df.sort_values(by=['TIMESTAMP'], inplace=True, ascending=False)

    df['Date'] = df['TIMESTAMP'].dt.strftime('%b-%Y')
    list = df['Date'].unique().tolist()
    list = list[:9]

    df = df[df['Date'].str.contains('|'.join(list))]

    pivot = df.pivot_table(index=['SYMBOL'], columns=['Date'], values=['OPEN_INT_sum'], aggfunc='mean')
    pivot.columns = pivot.columns.droplevel(0)
    pivot = pivot.reset_index().rename_axis(None, axis=1)

    list.insert(0, 'SYMBOL')

    pivot = pivot[list]
    pivot = round(pivot)

    sheet3 = pivot.dropna(axis=0)
    print(list[1], list[3])

    sheet3['chng%'] = round(((sheet3[list[1]] - sheet3[list[3]]) / sheet3[list[3]]) * 100)

    max_all = sheet3.max(axis=1)
    max_three = sheet3[sheet3.columns[1:4]].max(axis=1)
    max_rem_six = sheet3[sheet3.columns[4:10]].max(axis=1)

    sheet3['Flag_max_OI_recent'] = np.where((max_all == max_three), True, False)
    sheet3['Pchange_max'] = round(((max_all - max_rem_six) * 100) / max_rem_six, 2)
    sheet3['stock_filtered'] = np.where(
        ((sheet3['chng%'] > 0) & (sheet3['Flag_max_OI_recent'] == True) & (sheet3['Pchange_max'] > 20)), 1, 0)

    sheet3.sort_values(by=['chng%', 'Pchange_max'], inplace=True, ascending=False)

    sheet4 = pivot[pivot.isna().any(axis=1)]
    sheet4['chng%'] = np.where(((sheet4[list[1]] != np.nan) & (sheet4[list[3]] != np.nan)),
                               round(((sheet4[list[1]] - sheet4[list[3]]) / sheet4[list[3]]) * 100), 0)

    sheet3.to_csv(RAW_DIR+ '\\fut_Change_OI_Monthly.csv', index=None)
    sheet4.to_csv(RAW_DIR + '\\fut_New_Change_OI_Monthly.csv', index=None)


def get_fao_participant(Hist_date=None):
    str_date = Hist_date
    # str_date = '17062021'
    # get and read csv
    print(str_date)

    dls = "https://archives.nseindia.com/content/nsccl/fao_participant_oi_{}.csv".format(str_date)
    urllib.request.urlretrieve(dls, RAW_DIR + "\\fao_participant_oi_{}.csv".format(str_date))  # For Python 3

    today_df = pd.read_csv(RAW_DIR + '\\fao_participant_oi_{}.csv'.format(str_date), skiprows=1)

    os.remove(RAW_DIR + '\\fao_participant_oi_{}.csv'.format(str_date))
    today_df = pd.DataFrame(today_df)
    today_df['Date'] = str_date
    today_df['Date'] = pd.to_datetime(str_date, format='%d%m%Y')
    today_df.drop(labels=[4], axis=0, inplace=True)  # remove row 'total'
    pd.set_option("display.max_rows", None, "display.max_columns", None)

    # difference
    today_df['Future_Index'] = today_df['Future Index Long'] - today_df['Future Index Short']
    today_df['Future_Stock'] = today_df['Future Stock Long'] - today_df['Future Stock Short\t']
    today_df['Option_Index_call'] = today_df['Option Index Call Long'] - today_df['Option Index Call Short']
    today_df['Option_Index_Put'] = today_df['Option Index Put Long'] - today_df['Option Index Put Short']
    today_df['Option_Stock_Call'] = today_df['Option Stock Call Long'] - today_df['Option Stock Call Short']
    today_df['Option_Stock_Put'] = today_df['Option Stock Put Long'] - today_df['Option Stock Put Short']

    today_FII_df = pd.DataFrame(
        today_df[today_df['Client Type'] == 'FII'][['Date', 'Client Type', 'Future Index Long', 'Future Index Short']])
    today_FII_df['fii_long_pos'] = today_FII_df['Future Index Long'] / (
            today_FII_df['Future Index Long'] + today_FII_df['Future Index Short'])
    today_FII_df = today_FII_df[['Date', 'fii_long_pos']]
    today_FII_df.fii_long_pos = today_FII_df['fii_long_pos'].round(5)

    client_fii_df = today_df[today_df['Client Type'] == 'FII']
    client_fii_df['Date'] = date
    client_fii_df = client_fii_df[["Date", "Future Stock Long", "Future Stock Short\t"]]
    client_fii_df = client_fii_df.rename(columns={"Date": "Date", "Future Stock Long": "FII_Future_Stock_Long",
                                                  "Future Stock Short\t": "FII_Future_Stock_Short"})
    client_fii_df['Date'] = pd.to_datetime(client_fii_df.Date, format='%Y-%m-%d')

    long_short_csv = pd.read_csv(PROCESSED_DIR + '\\long_short_hist.csv')
    long_short_csv['Date'] = pd.to_datetime(long_short_csv.Date, format='%d-%m-%Y')

    if client_fii_df['Date'].max() != long_short_csv['Date'].max():
        long_short_new = pd.concat([long_short_csv, client_fii_df])
        long_short = long_short_new
        long_short['Date'] = long_short['Date'].dt.strftime('%d-%m-%Y')
        long_short.to_csv(PROCESSED_DIR + '\\long_short_hist.csv', index=False)

    long_short_csv = pd.read_csv(PROCESSED_DIR + '\\long_short_hist.csv')
    long_short_csv['Date'] = pd.to_datetime(long_short_csv.Date, format='%d-%m-%Y')
    cash_data_hist = pd.read_csv(PROCESSED_DIR + '\\cash_data_historical.csv')
    cash_data_hist.drop(['FII_Future_Stock_Long', 'FII_Future_Stock_Short'], inplace=True, axis=1)
    cash_data_hist['Date'] = pd.to_datetime(cash_data_hist.Date, format='%d-%m-%Y')
    df_merge = pd.merge(cash_data_hist, long_short_csv, on=['Date'], how='left')
    df_merge['Date'] = df_merge['Date'].dt.strftime('%d-%m-%Y')
    df_merge.to_csv(PROCESSED_DIR + '\\cash_data_historical.csv', index=None)

    today_df = pd.pivot_table(today_df, values=['Future_Index', 'Future_Stock', 'Option_Index_call', 'Option_Index_Put',
                                                'Option_Stock_Call', 'Option_Stock_Put'], index=['Date'],
                              columns=['Client Type']).reset_index()
    today_df.columns = ['Date', 'Fut_Index_client', 'Fut_Index_DII', 'Fut_Index_FII', 'Fut_Index_Pro',
                        'Fut_Stock_client', 'Fut_Stock_DII',
                        'Fut_Stock_FII', 'Fut_Stock_Pro', 'Opt_Index_Put_client', 'Opt_Index_Put_DII',
                        'Opt_Index_Put_FII', 'Opt_Index_Put_Pro',
                        'Opt_Index_call_client', 'Option_Index_call_DII', 'Option_Index_call_FII',
                        'Option_Index_call_Pro', 'Opt_Stock_Call_client',
                        'Option_Stock_Call_DII', 'Option_Stock_Call_FII', 'Option_Stock_Call_Pro',
                        'Opt_Stock_Put_client', 'Option_Stock_Put_DII',
                        'Option_Stock_Put_FII', 'Option_Stock_Put_Pro']

    DailyAgg_OI_df = pd.read_csv(REPORT_DIR + '\\bhav_data_nse_fut_agg_oi.csv')
    DailyAgg_OI_df['TIMESTAMP'] = pd.to_datetime(DailyAgg_OI_df['TIMESTAMP'], format='%d-%m-%Y')
    DailyAgg_OI_df = DailyAgg_OI_df[DailyAgg_OI_df['SYMBOL'] == 'NIFTY']
    DailyAgg_OI_df.drop(['OPEN_INT_sum', 'CHG_IN_OI_sum', 'SYMBOL'], inplace=True, axis=1)
    DailyAgg_OI_df = DailyAgg_OI_df.rename(columns={"TIMESTAMP": "Date"})
    DailyAgg_OI_df.sort_values(by=['Date'], ascending=False, inplace=True)

    # fii_position
    today_df = pd.merge(today_df, today_FII_df, on='Date', how='left').round(decimals=5)

    # concate and write csv
    today_df_csv = today_df
    if not os.path.exists(PROCESSED_DIR + '\\fao_historical.csv'):
        today_df_csv['Date'] = today_df_csv['Date'].dt.strftime('%d-%m-%Y')
        today_df_csv.to_csv(PROCESSED_DIR + '\\fao_historical.csv', index=False)
    else:
        Historical = pd.read_csv(PROCESSED_DIR + '\\fao_historical.csv')
        Historical['Date'] = pd.to_datetime(Historical.Date, format='%d-%m-%Y')
        if today_df.Date.max() != Historical.Date.max():
            Historical = pd.concat([today_df, Historical])

            Historical_csv = Historical
            Historical_csv['Date'] = Historical_csv['Date'].dt.strftime('%d-%m-%Y')
            Historical_csv.to_csv(PROCESSED_DIR + '\\fao_historical.csv', index=None)
            Historical['Date'] = pd.to_datetime(Historical.Date, format='%d-%m-%Y')

    # dump data
    Historical = pd.merge(DailyAgg_OI_df, Historical, on='Date')

    cash_df = pd.read_csv(PROCESSED_DIR + '\\cash_data_historical.csv')[
        ['Date', 'FII_Net_Purchase/Sales', 'DII_Net_Purchase/Sales', 'FII_Future_Stock_Long', 'FII_Future_Stock_Short']]
    cash_df['Date'] = pd.to_datetime(cash_df['Date'], format='%d-%m-%Y')
    cash_df = cash_df.rename(columns={"Date": "Date", "FII_Net_Purchase/Sales": "Cash_Stock_FII",
                                      "DII_Net_Purchase/Sales": "Cash_Stock_DII",
                                      "FII_Future_Stock_Long": "FII_Fut_Stock_Long",
                                      "FII_Future_Stock_Short": "FII_Fut_Stock_Short"})
    df_merge = pd.merge(Historical, cash_df, on=['Date'], how='left')

    df_merge = df_merge[["Date", "Close_first", "Cash_Stock_DII", "Cash_Stock_FII",
                         "Fut_Index_client", "Fut_Index_DII", "Fut_Index_FII",
                         "Fut_Index_Pro", "Fut_Stock_client", "Fut_Stock_DII", "Fut_Stock_FII", "FII_Fut_Stock_Long",
                         "FII_Fut_Stock_Short", "Fut_Stock_Pro", "Opt_Index_Put_client",
                         "Opt_Index_Put_DII", "Opt_Index_Put_FII", "Opt_Index_Put_Pro",
                         "Opt_Index_call_client", "Option_Index_call_DII", "Option_Index_call_FII",
                         "Option_Index_call_Pro", "Opt_Stock_Call_client",
                         'Option_Stock_Call_DII', 'Option_Stock_Call_FII', 'Option_Stock_Call_Pro',
                         'Opt_Stock_Put_client', 'Option_Stock_Put_DII',
                         'Option_Stock_Put_FII', 'Option_Stock_Put_Pro', 'fii_long_pos'
                         ]]

    df_merge.to_csv(RAW_DIR+'\\fut_Fii_Stats.csv', index=None)

    # unique dates
    date_unique = Historical['Date'].unique()
    unique_date_frame = pd.DataFrame(date_unique)
    unique_date_frame.sort_values(by=0, ascending=False, inplace=True)
    Today_date = unique_date_frame[0].iloc[0]
    yesterday_date = unique_date_frame[0].iloc[1]

    # today yesterday analysis

    today_df = Historical.loc[Historical['Date'] == Today_date]
    today_df = today_df.reset_index()

    today_df.columns = ['0', 'Date', 'Close_first', 'Fut_Index_client', 'Fut_Index_DII', 'Fut_Index_FII',
                        'Fut_Index_Pro',
                        'Fut_Stock_client', 'Fut_Stock_DII',
                        'Fut_Stock_FII', 'Fut_Stock_Pro', 'Opt_Index_Put_client', 'Opt_Index_Put_DII',
                        'Opt_Index_Put_FII', 'Opt_Index_Put_Pro',
                        'Opt_Index_call_client', 'Option_Index_call_DII', 'Option_Index_call_FII',
                        'Option_Index_call_Pro', 'Opt_Stock_Call_client',
                        'Option_Stock_Call_DII', 'Option_Stock_Call_FII', 'Option_Stock_Call_Pro',
                        'Opt_Stock_Put_client', 'Option_Stock_Put_DII',
                        'Option_Stock_Put_FII', 'Option_Stock_Put_Pro', 'fii_long_pos']
    today_df.drop(labels=['0', 'Close_first'], axis=1, inplace=True)
    today_df['Date'] = pd.to_datetime(today_df.Date, format='%d-%m-%Y')

    yesterday_df = Historical.loc[Historical['Date'] == yesterday_date]
    yesterday_df = yesterday_df.reset_index()
    yesterday_df.columns = ['0', 'Date', 'Close_first', 'Fut_Index_client', 'Fut_Index_DII', 'Fut_Index_FII',
                            'Fut_Index_Pro',
                            'Fut_Stock_client', 'Fut_Stock_DII',
                            'Fut_Stock_FII', 'Fut_Stock_Pro', 'Opt_Index_Put_client', 'Opt_Index_Put_DII',
                            'Opt_Index_Put_FII', 'Opt_Index_Put_Pro',
                            'Opt_Index_call_client', 'Option_Index_call_DII', 'Option_Index_call_FII',
                            'Option_Index_call_Pro', 'Opt_Stock_Call_client',
                            'Option_Stock_Call_DII', 'Option_Stock_Call_FII', 'Option_Stock_Call_Pro',
                            'Opt_Stock_Put_client', 'Option_Stock_Put_DII',
                            'Option_Stock_Put_FII', 'Option_Stock_Put_Pro', 'fii_long_pos']
    yesterday_df['Date'] = pd.to_datetime(yesterday_df.Date, format='%d-%m-%Y')
    yesterday_df.drop(labels=['0', 'Close_first'], axis=1, inplace=True)

    # subtracting data frames
    diff = today_df.subtract(yesterday_df, fill_value=0)
    diff.drop(labels=['Date', 'fii_long_pos'], axis=1, inplace=True)
    diff['Date'] = Today_date
    diff['Date'] = pd.to_datetime(diff['Date'], format='%d-%m-%Y')
    diff['yesterday_date'] = yesterday_date
    diff['yesterday_date'] = pd.to_datetime(diff['yesterday_date'], format='%d-%m-%Y')
    diff['fii_long_pos_today'] = today_df['fii_long_pos']
    diff['fii_long_pos_yesterday'] = yesterday_df['fii_long_pos']
    diff['Date'] = diff['Date'].dt.strftime('%d-%m-%Y')
    diff['yesterday_date'] = diff['yesterday_date'].dt.strftime('%d-%m-%Y')

    # diff.drop(['Close_first'], inplace=True, axis=1)
    diff.to_csv(METADATA_DIR + '\\fao_daily.csv', index=False)


def get_fii_stats(Hist_date=None):
    if Hist_date:
        str_date_new = Hist_date
    str_date_new = fii_stats_date

    # str_date_new = '17-May-2021'
    dls = "https://archives.nseindia.com/content/fo/fii_stats_" + str_date_new + ".xls".format(str_date_new)
    # dls = "https://archives.nseindia.com/content/fo/fii_stats_"
    urllib.request.urlretrieve(dls, RAW_DIR + "\\fii_stats_" + str_date_new + ".xls".format(str_date_new))
    fii_stats = pd.read_excel(RAW_DIR + '\\fii_stats_' + str_date_new + '.xls'.format(str_date_new), skiprows=2).fillna(
        0)
    fii_stats = pd.DataFrame(fii_stats)

    fii_stats['date'] = str_date_new
    fii_stats['date'] = pd.to_datetime(str_date_new, format='%d-%b-%Y')

    fii_stats = fii_stats.loc[(fii_stats['No. of contracts'] != 0)].reset_index()
    fii_stats.columns = ['index', 'FUTURES', 'Buy_no_contract', 'Buy_amt_contract', 'Sell_no_contract',
                         'Sell_amt_contract', 'no_contract', 'amount', 'date']
    fii_stats['Val_Diff'] = fii_stats['Buy_amt_contract'] - fii_stats['Sell_amt_contract']
    fii_stats = fii_stats[['date', 'FUTURES', 'Val_Diff']]
    filt = fii_stats["FUTURES"].isin(["INDEX FUTURES", "STOCK FUTURES"])
    fii_stats = fii_stats[filt]
    fii_stats = pd.DataFrame(fii_stats)
    fii_stats = pd.pivot_table(fii_stats, values=['Val_Diff'], index=['date'], columns=['FUTURES']).reset_index()
    fii_stats.columns = ['date', 'Val_Diff_Index_FUT', 'Val_Diff_Stock_FUT']

    # concate and write csv
    if not os.path.exists(PROCESSED_DIR + '\\fii_stats_historical.csv'):
        fii_stats['date'] = fii_stats['date'].dt.strftime('%d-%m-%Y')
        fii_stats.to_csv(PROCESSED_DIR + '\\fii_stats_historical.csv', index=False)
    else:
        fii_stats_historical = pd.read_csv(PROCESSED_DIR + '\\fii_stats_historical.csv')
        fii_stats_historical['date'] = pd.to_datetime(fii_stats_historical['date'], format='%d-%m-%Y')
        if fii_stats.date.max() != fii_stats_historical.date.max():
            fii_stats_historical = pd.concat([fii_stats, fii_stats_historical])
            fii_stats_historical['date'] = fii_stats_historical['date'].dt.strftime('%d-%m-%Y')
            fii_stats_historical.to_csv(PROCESSED_DIR + '\\fii_stats_historical.csv', index=None)

    # concate fii_historical and fao_historical
    fii_stats_historical = pd.read_csv(PROCESSED_DIR + '\\fii_stats_historical.csv').reset_index()
    fii_stats_historical.columns = ['0', 'Date', 'Val_Diff_Index_FUT', 'Val_Diff_Stock_FUT']
    fii_stats_historical.drop(labels=['0'], axis=1, inplace=True)

    fii_stats_historical['Date'] = pd.to_datetime(fii_stats_historical.Date, format='%d-%m-%Y')

    Historical = pd.read_csv(METADATA_DIR + '\\fao_daily.csv')
    Historical['Date'] = pd.to_datetime(Historical['Date'], format='%d-%m-%Y')
    Historical['yesterday_date'] = pd.to_datetime(Historical['yesterday_date'], format='%d-%m-%Y')

    temp = pd.merge(Historical, fii_stats_historical, on='Date', how='left')

    # calculate decimal
    temp['decimal_index'] = temp['Val_Diff_Index_FUT'] / temp['Fut_Index_FII']
    temp['decimal_stock'] = temp['Val_Diff_Stock_FUT'] / temp['Fut_Stock_FII']

    temp["Fut_Index_client_new"] = temp["Fut_Index_client"] * temp["decimal_index"]
    temp["Fut_Index_DII_new"] = temp["Fut_Index_DII"] * temp["decimal_index"]
    temp["Fut_Index_Pro_new"] = temp["Fut_Index_Pro"] * temp["decimal_index"]

    temp["Fut_Stock_client_new"] = temp["Fut_Stock_client"] * temp["decimal_stock"]
    temp["Fut_Stock_DII_new"] = temp["Fut_Stock_DII"] * temp["decimal_stock"]
    temp["Fut_Stock_Pro_new"] = temp["Fut_Stock_Pro"] * temp["decimal_stock"]
    temp = pd.DataFrame(temp)
    temp = temp.round({"decimal_index": 2, "decimal_stock": 2, "Fut_Index_client_new": 2, "Fut_Index_DII_new": 2
                          , "Fut_Index_Pro_new": 2, "Fut_Stock_client_new": 2, "Fut_Stock_DII_new": 2,
                       "Fut_Stock_Pro_new": 2})
    temp.sort_values(by=['Date'], ascending=False, inplace=True)
    temp['Date'] = temp['Date'].dt.strftime('%d-%m-%Y')

    temp.to_csv(RAW_DIR+ '\\fut_cdfp_value.csv', index=None)


def screenshot():
    app = xw.apps.active
    app.quit()

    if os.path.exists(REPORT_DIR + "\\fut_OI_historical.xlsm"):
        xl = win32com.client.Dispatch("Excel.Application")
        xl.Workbooks.Open(REPORT_DIR + "\\fut_OI_historical.xlsm", ReadOnly=1)
        xl.Application.Run("fut_OI_historical.xlsm!module1.slicer_snap")
        xl.Application.Run("fut_OI_historical.xlsm!module1.historical_snap")
        xl.Application.Quit()
        del xl


def load_csv_to_excel():
    data_excel_file = REPORT_DIR + "\\fut_OI_historical.xlsm"
    wb = xw.Book(data_excel_file)

    fii_stats = pd.read_csv(RAW_DIR + '\\fut_Fii_Stats.csv')
    sheet_oi_single = wb.sheets('Fii_Stats')
    sheet_oi_single.range("A1").options(index=None).value = fii_stats

    cdfp = pd.read_csv(RAW_DIR + '\\fut_cdfp_value.csv')
    sheet_oi_single = wb.sheets('cdfp_value')
    sheet_oi_single.range("L3").options(index=None).value = cdfp

    oi_monthly = pd.read_csv(RAW_DIR + '\\fut_Change_OI_Monthly.csv')
    sheet_oi_single = wb.sheets('Change_OI_Monthly')
    sheet_oi_single.clear()
    sheet_oi_single.range("A1").options(index=None).value = oi_monthly

    new_oi_monthly = pd.read_csv(RAW_DIR + '\\fut_New_Change_OI_Monthly.csv')
    sheet_oi_single = wb.sheets('New_Change_OI_Monthly')
    sheet_oi_single.clear()
    sheet_oi_single.range("A1").options(index=None).value = new_oi_monthly

    Today_yesterday = pd.read_csv(RAW_DIR + '\\fut_Today_yesterday.csv')
    sheet_oi_single = wb.sheets('Today_yesterday')
    sheet_oi_single.clear()
    sheet_oi_single.range("A7").options(index=None).value = Today_yesterday

    Historical_data = pd.read_csv(RAW_DIR + '\\fut_Historical_data.csv')
    sheet_oi_single = wb.sheets('Historical_data')
    sheet_oi_single.range("A7").options().value = Historical_data

    Report_data = pd.read_csv(RAW_DIR + '\\fut_Report_data.csv')
    sheet_oi_single = wb.sheets('Report_data')
    sheet_oi_single.range("A1").options(index=None).value = Report_data

    AvgMinMaxOI = pd.read_csv(RAW_DIR + '\\fut_AvgMinMaxOI.csv')
    sheet_oi_single = wb.sheets('AvgMinMaxOI')
    sheet_oi_single.range("A1").options(index=None).value = AvgMinMaxOI

    Last_Week_OI = pd.read_csv(RAW_DIR + '\\fut_Last_Week_OI.csv')
    sheet_oi_single = wb.sheets('Last_Week_OI')
    sheet_oi_single.range("A1").options(index=None).value = Last_Week_OI

    Today_OI = pd.read_csv(RAW_DIR + '\\fut_Today_OI.csv')
    sheet_oi_single = wb.sheets('Today_OI')
    sheet_oi_single.range("A1").options(index=None).value = Today_OI

    DailyAgg_OI = pd.read_csv(RAW_DIR + '\\fut_DailyAgg_OI.csv')
    sheet_oi_single = wb.sheets('DailyAgg_OI')
    sheet_oi_single.clear()
    sheet_oi_single.range("A1").options(index=None).value = DailyAgg_OI  # df_today

    MWPL = pd.read_csv(RAW_DIR + '\\fut_MWPL.csv')
    sheet_oi_single = wb.sheets('MWPL')
    sheet_oi_single.range("A1").options(index=None).value = MWPL

    fut_Index = pd.read_csv(RAW_DIR + '\\fut_Index.csv')
    sheet_oi_single = wb.sheets('Index')
    sheet_oi_single.range('A2:F40').clear()
    sheet_oi_single.range("A1").options(index=None).value = fut_Index.sort_values(['SYMBOL', 'TIMESTAMP'])

    sheet_oi_single = wb.sheets('BN_Nifty_Major_opt')
    sheet_oi_single.clear()
    BN_Nifty_Major_opt = pd.read_csv(RAW_DIR + '\\BN_Nifty_Major_opt_tab.csv')
    sheet_oi_single.range("A2").options(index=None).value = BN_Nifty_Major_opt


    sheet_oi_single = wb.sheets('itm_opt')
    sheet_oi_single.clear()
    itm_opt = pd.read_csv(RAW_DIR + '\\itm_opt_tab.csv')
    sheet_oi_single.range("A1").options(index=None).value = itm_opt

    sheet_oi_single = wb.sheets('indx_comm')
    sheet_oi_single.clear()
    indx_comm = pd.read_csv(RAW_DIR + '\\daily_ind_comm.csv')
    indx_comm['Date'] = pd.to_datetime(indx_comm['Date'], format='%d-%m-%Y')
    indx_comm['Date'] = indx_comm['Date'].dt.strftime('%d-%m-%Y')
    sheet_oi_single.range("A1").options(index=None).value = indx_comm

    sheet_oi_single = wb.sheets('SGX')
    sheet_oi_single.clear()
    Agg_SGX = pd.read_csv(PROCESSED_DIR + '\\' + 'Agg_SGX_Nifty_Future.csv')
    sheet_oi_single.range("A1").options(index=None).value = Agg_SGX

    sheet_oi_single = wb.sheets('Foreign_data')
    sheet_oi_single.clear()
    foreign_data = pd.read_csv(r'\\192.168.41.190\chayan\Taiwan_korea\HistoricalTWDKRX.csv')
    foreign_data.to_csv(PROCESSED_DIR + '\\HistoricalTWDKRX.csv', index=None)
    sheet_oi_single.range("A1").options(index=None).value = foreign_data
    wb.save()


def sendmail():
    EMAIL_ADDRESS = 'researchequity10@gmail.com'
    EMAIL_PASSWORD = 'noxtajxjdmzdjlkb'

    contacts = ['researchequity10@gmail.com', 'engineers1030164@gmail.com']
    msg = EmailMessage()
    msg['Subject'] = 'Futures Report as of ' + str(last_date)
    msg['From'] = EMAIL_ADDRESS
    msg['To'] = ['rohnkoria@gmail.com', 'engineers1030164@gmail.com', 'vijitramavat@gmail.com']

    # now create a Content-ID for the image
    image_cid = make_msgid()
    image2_cid = make_msgid()
    image3_cid = make_msgid()
    image4_cid = make_msgid()
    image5_cid = make_msgid()
    image6_cid = make_msgid()
    image7_cid = make_msgid()
    image8_cid = make_msgid()
    image9_cid = make_msgid()
    image10_cid = make_msgid()
    image11_cid = make_msgid()
    image12_cid = make_msgid()

    msg.add_alternative("""\
    <!DOCTYPE html>
    <html>
        <body>
            <h1 style="color:SlateGray;">DAILY CHN 5%</h1>
            <img src="cid:{image_cid}">
            <h1 style="color:SlateGray;">WEEKLY CHN 10%</h1>
            <img src="cid:{image2_cid}">
            <h1 style="color:SlateGray;">OI HIGHEST</h1>
            <img src="cid:{image3_cid}">
            <h1 style="color:SlateGray;">CLient Today MWPL</h1>
            <img src="cid:{image4_cid}">
            <h1 style="color:SlateGray;">Client Historical MWPL</h1>
            <img src="cid:{image5_cid}">
            <h1 style="color:SlateGray;">Cdfp Value</h1>
            <img src="cid:{image6_cid}">
            <h1 style="color:SlateGray;">Fii Stats</h1>
            <img src="cid:{image7_cid}">
            <h1 style="color:SlateGray;">Index Buildup</h1>
            <img src="cid:{image8_cid}">
            <h1 style="color:SlateGray;">BN_Nifty_Major_opt</h1>
            <img src="cid:{image9_cid}">
            <img src="cid:{image11_cid}">
            <h1 style="color:SlateGray;">ITM_opt</h1>
            <img src="cid:{image10_cid}">
            <h1 style="color:SlateGray;">Change_OI_Monthly</h1>
            <img src="cid:{image12_cid}">
        </body>
    </html>
    """.format(image_cid=image_cid[1:-1], image2_cid=image2_cid[1:-1], image3_cid=image3_cid[1:-1],
               image4_cid=image4_cid[1:-1]
               , image5_cid=image5_cid[1:-1], image6_cid=image6_cid[1:-1], image7_cid=image7_cid[1:-1],
               image8_cid=image8_cid[1:-1], image10_cid=image10_cid[1:-1], image9_cid=image9_cid[1:-1]
               , image11_cid=image11_cid[1:-1], image12_cid=image12_cid[1:-1]), subtype='html')

    with open(SS_DIR + '//Daily_Chn_5.jpg', 'rb') as img:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image_cid)

    with open(SS_DIR + '//Weekly_chn_10.jpg', 'rb') as img2:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img2.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img2.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image2_cid)

    with open(SS_DIR + '//IS_OI_Highiest.jpg', 'rb') as img3:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img3.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img3.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image3_cid)

    with open(SS_DIR + '//Today_yesterday.jpg', 'rb') as img4:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img4.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img4.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image4_cid)

    with open(SS_DIR + '//Historical.jpg', 'rb') as img5:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img5.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img5.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image5_cid)

    with open(SS_DIR + '//fao_report.jpg', 'rb') as img6:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img6.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img6.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image6_cid)

    with open(SS_DIR + '//Fii_Stats.jpg', 'rb') as img7:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img7.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img7.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image7_cid)

    with open(SS_DIR + '//Index_buildup.jpg', 'rb') as img8:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img8.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img8.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image8_cid)

    with open(SS_DIR + '//BN_Nifty_Major_opt.jpg', 'rb') as img9:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img9.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img9.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image9_cid)

    with open(SS_DIR + '//itm_opt.jpg', 'rb') as img10:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img10.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img10.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image10_cid)

    with open(SS_DIR + '//BN_Nifty_Major_opt_2.jpg', 'rb') as img11:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img11.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img11.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image11_cid)

    with open(SS_DIR + '//Change_OI_Monthly.jpg', 'rb') as img12:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img12.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img12.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image12_cid)

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        smtp.send_message(msg)


def get_historical_file(date=None):
    print(date)
    if date:

        if date.day < 10 and date.month < 10:
            str_date = ''.join(['0', str(date.day), '0', str(date.month), str(date.year)])

        elif date.day < 10:
            str_date = ''.join(['0', str(date.day), str(date.month), str(date.year)])
        elif date.month < 10:
            str_date = ''.join([str(date.day), '0', str(date.month), str(date.year)])
        else:
            str_date = ''.join([str(date.day), str(date.month), str(date.year)])

        global mon_str, year_str, day_str
        mon_str = date.strftime('%b')
        year_str = str_date[-4:]
        day_str = str_date[:2]
        print(str_date)

        str_date_new = f'{day_str}-{mon_str}-{year_str}'
        # get_fao_participant(str_date)

        # For getting Option EOD data
        if 1 == 1:
            mon_str = mon_str.upper()
            try:
                bhav_file_path, str_date = get_nse_Bhav_file(str_date)
                unzip_folder(bhav_file_path, LOG_FILE_NAME)
                if bhav_file_path:
                    update_nse_fut_master_file(bhav_file_path.split('.zip')[0], str_date)
            except Exception as e:
                LOG_insert(LOG_FILE_NAME, str(e) + "File Not Available On This Date", logging.ERROR)
                print(str_date, "found in exception")
                pass

            return str_date


def cash():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9", "Accept-Encoding": "gzip, deflate, br"}
    s = requests.session()
    s.get('https://www.nseindia.com/get-quotes/derivatives?symbol=' + 'NIFTY', headers=headers)
    r = s.get('https://www.nseindia.com/api/fiidiiTradeReact', headers=headers).json()
    dii_df = pd.DataFrame()
    fii_df = pd.DataFrame()

    # for i in range(len(r)):
    dii_df['category'] = [r[0]['category']]
    dii_df['Date'] = [r[0]['date']]
    dii_df['DII_Gross_Purchase'] = [r[0]['buyValue']]
    dii_df['DII_Gross_Sales'] = [r[0]['sellValue']]
    dii_df['DII_Net_Purchase/Sales'] = [r[0]['netValue']]
    fii_df['category_I'] = [r[1]['category']]
    fii_df['dat'] = [r[1]['date']]
    fii_df['FII_Gross_Purchase'] = [r[1]['buyValue']]
    fii_df['FII_Gross_Sales'] = [r[1]['sellValue']]
    fii_df['FII_Net_Purchase/Sales'] = [r[1]['netValue']]
    fii_df['dat'] = pd.to_datetime(fii_df['dat'], format='%d-%b-%Y')
    dii_df['Date'] = pd.to_datetime(dii_df['Date'], format='%d-%b-%Y')
    cash_all_df = pd.merge(dii_df, fii_df, left_on='Date', right_on='dat')
    cash_all_df.drop(['category', 'category_I', 'dat'], inplace=True, axis=1)
    cash_all_df = cash_all_df[["Date", "FII_Gross_Purchase", "FII_Gross_Sales", "FII_Net_Purchase/Sales",
                               "DII_Gross_Purchase", "DII_Gross_Sales", "DII_Net_Purchase/Sales"]]

    if not os.path.exists(PROCESSED_DIR + '\\cash_data_historical.csv'):
        cash_all_df['Date'] = cash_all_df['Date'].dt.strftime('%d-%m-%Y')
        cash_all_df.to_csv(PROCESSED_DIR + '\\cash_data_historical.csv', index=False)
    else:
        Historical = pd.read_csv(PROCESSED_DIR + '\\cash_data_historical.csv')
        Historical['Date'] = pd.to_datetime(Historical['Date'], format='%d-%m-%Y')
        max_date = Historical['Date'].max()
        cash_all_df = cash_all_df[cash_all_df['Date'] > max_date]
        Historical = pd.concat([cash_all_df, Historical])
        Historical['Date'] = Historical['Date'].dt.strftime('%d-%m-%Y')
        Historical.to_csv(PROCESSED_DIR + '\\cash_data_historical.csv', index=False)


def call_function():
    date_today = datetime.date.today()
    HISTORICAL_DATA = 0
    try:
        if HISTORICAL_DATA == 1:
            start_date = datetime.date(day=1, month=1, year=2020)
            end_date = datetime.date(day=22, month=11, year=2021)
            totaldays = (end_date - start_date).days
            for i in range(totaldays + 1):

                date = start_date + datetime.timedelta(days=i)
                try:
                    print(date)
                    get_historical_bhav_file(date)

                except:
                    print('No data found for date')
                    continue
        else:

            cash()
            print('cash_data')

            get_historical_bhav_file()
            print('get_historical_bhav_file')

            load_fut_dashboard()
            print('load_fut_dashboard')

            change_oi_monthly()
            print('change_oi_monthly')

            get_fao_participant(str_date)
            print('get_fao_participant')

            try:
                mwpl_cli_report()
                print('mwpl_cli_report')
            except Exception as e:
                print(e)

            get_fii_stats()
            print('get_fii_stats')

            load_csv_to_excel()
            print('loading_excel_complete')

            screenshot()
            print('screenshot')
            try:
                print('sendmail')
                sendmail()
            except:
                pass

        execution_status('pass', file, '', str_date, 1)
        quit()

    except Exception as e:
        print(e)
        execution_status(str(e), file, logging.ERROR, str_date, 0)
        sendmail_err(file, e)
        pass


if __name__ == '__main__':
    import get_api_nse_fno_eod
    print('Options data')
    call_function()
