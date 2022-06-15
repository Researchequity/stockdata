import concurrent.futures
from filepath import *
from logger_file import *
import glob
from zipfile import ZipFile
import pandas as pd
import dateutil
import datetime
from icecream import ic
from selenium import webdriver
import smtplib
from email.message import EmailMessage
import random
from functools import reduce
import numpy as np
import warnings
warnings.simplefilter(action="ignore", category=Warning)
import threading
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys


DRIVER_PATH = "/home/pc/bse/chromedriver"
header = {
  "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36",
  "X-Requested-With": "XMLHttpRequest"
}
nationality = {
    'A1) Indian': ['Individuals/Hindu undivided Family', 'Central Government/ State Government(s)', 'Any Other (specify)'],
    'A2) Foreign': ['Individuals (NonResident Individuals/ Foreign Individuals)', 'Any Other (specify)', 'None']
}
categories = {
  'B1) Institutions': ['Mutual Funds/', 'Alternate Investment Funds', 'Foreign Portfolio Investors',
                       'Financial Institutions/ Banks', 'Any Other (specify)',
                       'Insurance Companies', 'Financial Institutions/ Banks',
                       'Venture Capital Funds', 'Provident Funds/ Pension Funds'],
  'B2) Central Government/ State Government(s)/ President of India': ['Central Government/ State Government(s)/ President of India',
                                                                      'None', 'None',
                                                                      'None', 'None', 'None',
                                                                      'None', 'None', 'None'],
  'B3) Non-Institutions': ['Individual share capital upto Rs. 2 Lacs',
                           'Individual share capital in excess of Rs. 2 Lacs',
                           'NBFCs registered with RBI', 'Employee Trusts', 'Any Other (specify)',
                           'None', 'None', 'None', 'None'],
}
qtr_list = ['March 2025','June 2025','September 2025','December 2025','March 2024','June 2024','September 2024','December 2024',
                'March 2023','June 2023','September 2023','December 2023','March 2022','June 2022','September 2022','December 2022',
               'March 2021','June 2021','September 2021','December 2021','March 2020','June 2020','September 2020','December 2020',
                'March 2019','June 2019','September 2019','December 2019','March 2018','June 2018','September 2018','December 2018',
                'March 2017','June 2017','September 2017', 'December 2017','March 2016','June 2016','September 2016','December 2016',
                'March 2015','June 2015','September 2015','December 2015']
threads = []
avg_all = []


def unzip_folder(bhav_file_path, LOG_FILE_NAME):
    try:
        zf = ZipFile(bhav_file_path)

        zf.extractall(RAW_DIR)
        zf.close()
    except Exception as e:
        LOG_insert(LOG_FILE_NAME, str(e) + " Got error while unzipping", logging.ERROR)


def deleting_zipped_folder(LOG_FILE_NAME):
    try:
        os.chdir(BASE_DIR + '/raw/')
        for file in glob.glob("*.zip"):
            print(file, "file for deleting")
            os.remove(file)
    except Exception as e:
        LOG_insert(LOG_FILE_NAME, str(e) + " Got error while deleting zipped file", logging.ERROR)


def deleting_csv_files(bhav_file_path, LOG_FILE_NAME):
    try:
        if os.path.exists(bhav_file_path):
            os.remove(bhav_file_path)
    except Exception as e:
        LOG_insert(LOG_FILE_NAME, str(e) + " Got error while deleting csv file", logging.ERROR)


def get_market_prev_date_fut_date(date_today):
    hol_df = pd.read_csv(METADATA_DIR + '\\holiday.csv')
    holiday_list = [dateutil.parser.parse(dat).date() for dat in hol_df['Holiday'].values]

    "Weekday Return day of the week, where Monday == 0 ... Sunday == 6."
    # while date_today in holiday_list or date_today.weekday() == 6 or date_today.weekday() == 5:
    #     date_today = date_today - datetime.timedelta(days=1)
    # ic(date_today)
    prev_date = date_today - datetime.timedelta(days=1)
    while prev_date in holiday_list or prev_date.weekday() == 6 or prev_date.weekday() == 5:
        prev_date = prev_date - datetime.timedelta(days=1)
    ic(prev_date)
    fut_date = date_today + datetime.timedelta(days=1)
    while fut_date in holiday_list or fut_date.weekday() == 6 or fut_date.weekday() == 5:
        fut_date = fut_date + datetime.timedelta(days=1)
    ic(fut_date)

    return date_today, prev_date, fut_date


def browser_profile():
    driver = webdriver.ChromeOptions()
    driver.add_experimental_option('prefs', {'intl.accept_languages': 'en,en_US'})
    driver.to_capabilities()
    return driver


def sendmail_err(file, err):
    date_today = datetime.date.today()
    EMAIL_ADDRESS = 'researchequity10@gmail.com'
    EMAIL_PASSWORD = 'noxtajxjdmzdjlkb'

    msg = EmailMessage()
    msg['Subject'] = 'Error:' + str(file) + '-' + str(err) + '-' + str(date_today)
    msg['From'] = EMAIL_ADDRESS
    msg['To'] = ['rohnkoria@gmail.com', 'engineers1030164@gmail.com', 'kinjalchauhan1999@gmail.com']

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        smtp.send_message(msg)


def superstar_check(final_df):
    ## change superstar column name to 'lower_names'
    superstar_name_df = pd.read_csv(METADATA_DIR + '\\superstar_name_new.csv')
    f_name = superstar_name_df['f_name'].str.lower()
    f_name = f_name.to_list()
    superstar_df = pd.DataFrame()
    for sup in f_name:
        temp_df = final_df[final_df['lower_names'].str.contains(sup)]
        superstar_df = pd.concat([superstar_df, temp_df])

    return superstar_df


def get_nationality_category_promoter(df):
    cols = list(df['Category of shareholder'])
    for col in cols:
        if str(col).find('\xa0'):
            cols[cols.index(col)] = ' '.join(str(col).split('\xa0'))
    temp_list = [None]*df['Category of shareholder'].shape[0]
    temp_cate_list = [None] * df['Category of shareholder'].shape[0]

    try:
        temp_list[cols.index(list(nationality.keys())[0])] = str(list(nationality.keys())[0]).split(' ')[1]
        temp_cate_list[cols.index(list(nationality.keys())[0])] = str(list(nationality.keys())[0]).split(' ')[1]
    except:
        pass
    try:
        temp_list[cols.index(list(nationality.keys())[1])] = str(list(nationality.keys())[1]).split(' ')[1]
        temp_cate_list[cols.index(list(nationality.keys())[1])] = str(list(nationality.keys())[1]).split(' ')[1]
    except:
        pass

    for i in range(2):
        for j in range(2):
            try:
                temp_cate_list[cols.index(list(nationality.values())[i][j])] = cols[cols.index(list(nationality.values())[i][j])]
            except:
                continue

    for element in temp_list:
        if element is not None:
            continue
        else:
            temp_list[temp_list.index(element)] = temp_list[temp_list.index(element)-1]

    for element in temp_cate_list:
        if element is not None:
            continue
        else:
            temp_cate_list[temp_cate_list.index(element)] = temp_cate_list[temp_cate_list.index(element) - 1]
    return temp_list, temp_cate_list


def get_nationality_category_public(df):
    cols = list(df['Category & Name of the Shareholders'])
    for col in cols:
        if str(col).find('\xa0'):
            cols[cols.index(col)] = ' '.join(str(col).split('\xa0'))
    temp_list = [None]*df['Category & Name of the Shareholders'].shape[0]
    temp_cate_list = [None] * df['Category & Name of the Shareholders'].shape[0]
    try:
        temp_list[cols.index(list(categories.keys())[0])] = str(list(categories.keys())[0]).split(') ')[1]
        temp_cate_list[cols.index(list(categories.keys())[0])] = str(list(categories.keys())[0]).split(') ')[1]
    except:
        pass
    try:
        temp_list[cols.index(list(categories.keys())[1])] = str(list(categories.keys())[1]).split(') ')[1]
        temp_cate_list[cols.index(list(categories.keys())[1])] = str(list(categories.keys())[1]).split(') ')[1]
    except:
        pass
    try:
        temp_list[cols.index(list(categories.keys())[2])] = str(list(categories.keys())[2]).split(') ')[1]
        temp_cate_list[cols.index(list(categories.keys())[2])] = str(list(categories.keys())[2]).split(') ')[1]
    except:
        pass

    temp_col = reduce(lambda x,y:x+y, list(categories.values()))
    indices = [{i:x} for i, x in enumerate(cols) if x in temp_col]

    try:
        for index in indices:
            temp_cate_list[list(index.keys())[0]] = list(index.values())[0]
    except:
        pass

    for element in temp_list:
        if element is not None:
            continue
        else:
            temp_list[temp_list.index(element)] = temp_list[temp_list.index(element)-1]

    for element in temp_cate_list:
        if element is not None:
            continue
        else:
            temp_cate_list[temp_cate_list.index(element)] = temp_cate_list[temp_cate_list.index(element) - 1]
    return temp_list, temp_cate_list


def add_details_in_headers_with_noentries():
    df = pd.read_csv(METADATA_DIR + '\\final.csv')
    index = df.index[df[df.columns.values[-1]].apply(np.isnan)]
    df = df.drop(index)
    temp_col = reduce(lambda x, y: x + y, list(categories.values()))
    cols = list(df['Category of shareholder'])
    i = 0
    for _ in range(len(cols)):
        if i >= len(cols):
            break
        # elif cols[i] in temp_col and cols[i + 1] in temp_col:
        elif cols[i] in temp_col and cols[i] in temp_col:
            Filter_df = df[df.index.isin([i])]
            # Filter_df.loc[i, df.columns.get_loc('Category of shareholder')] = cols[i] + '_details'
            df = pd.concat([df.iloc[:i], Filter_df, df.iloc[i:]]).reset_index(drop=True)
            df.loc[i+1, 'Category of shareholder'] = cols[i] + '_details'
            cols.insert(i, cols[i])
            i = i + 2
        else:
            i = i + 1
    header_list = []
    for j in range(df.shape[0]):
        if df.loc[j, 'Category'] == df.loc[j, 'Category of shareholder']:
            header_list.append(1)
        else:
            header_list.append(0)
    df.insert(loc=9, column='Header', value=header_list)
    index = df.index[df[df.columns.values[-1]].apply(np.isnan)]
    df = df.drop(index)
    df = df[df.columns.values[2:]]
    return df


def get_random_wait(initial_limit=1, upper_limit=5):
    return random.randint(initial_limit, upper_limit)


def avg_thread(stock, final_df):
    try:
        df_stock_perline = final_df[final_df['avg_stock'] == stock]

        # slow mean via last 30 session
        df_stock_perline = df_stock_perline[-30:]
        df_mean = df_stock_perline["avg_col"].mean()
        df_mean = int(pd.DataFrame(np.where(df_stock_perline['avg_col'] < (2 * df_mean), df_stock_perline['avg_col']
                                            , np.nan)).mean())
        df_stock_perline.loc[df_stock_perline.index[-1], 'avg_mean_slow'] = round(df_mean)

        # Fast mean via last 8 session
        df_stock_perline = df_stock_perline[-8:]
        df_mean = df_stock_perline["avg_col"].mean()
        df_mean = int(
            pd.DataFrame(np.where(df_stock_perline['avg_col'] < (2 * df_mean), df_stock_perline['avg_col'], np.nan)).mean())
        df_stock_perline.loc[df_stock_perline.index[-1], 'avg_mean_fast'] = round(df_mean)

        avg_all.append(df_stock_perline.tail(1))
    except:
        pass


def average(file_df, pass_date):
    final_df = file_df[file_df['avg_date'] <= str(pass_date)]

    #get slow and fast mean
    uniqueValues = final_df['avg_stock'].unique()
    # uniqueValues = ['20MICRONS', '3IINFOTECH', '63MOONS', 'ADANIENT', 'ADANIPORTS']

    for stock in uniqueValues:
        ic(stock)
        t1 = threading.Thread(target=avg_thread, args=(stock, final_df))
        t1.start()
        threads.append(t1)

    for process in threads:
        process.join()
    # with concurrent.futures.ProcessPoolExecutor() as executor:
    #     results = [executor.submit(avg_thread, (stock, final_df)) for stock in uniqueValues]

    df_stock = pd.concat(avg_all)
    df_stock['avg_norm_mean'] = np.where(df_stock['avg_mean_slow'] >= df_stock['avg_mean_fast'],
                                             df_stock['avg_mean_fast'], df_stock['avg_mean_slow'])
    df_stock.drop(['avg_mean_slow', 'avg_mean_fast', 'avg_stock', 'avg_date', 'avg_col'], inplace=True, axis=1)

    return df_stock


def execution_status(e, file, err, str_date, num):
    temp = [e, file, err, str_date, num]
    exe_stats = pd.DataFrame(temp)
    exe_stats_csv = pd.DataFrame(exe_stats.values.reshape(-1, 5),
                                        columns=['error', 'file_name', 'log_err', 'date', 'status'])

    exe_stats_csv['run_date_time'] = datetime.datetime.today()
    exe_stats_csv['run_date_time'] = exe_stats_csv['run_date_time'].dt.strftime('%d-%m-%Y %H:%M')
    exe_stats_csv.to_csv(LOG_DIR + '\\exe_stats.csv', index=False, header=False, mode='a')
