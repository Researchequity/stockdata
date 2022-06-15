import pandas as pd
import requests
import urllib
from urllib.request import urlopen
from utils import *


date_today = datetime.date.today() #- datetime.timedelta(days=1)
print(date_today)

date_today, prev_date, fut_date = get_market_prev_date_fut_date(date_today)

file = os.path.basename(__file__)
LOG_FILE_NAME = str(LOG_DIR)+"\\get_BHAV_FILE_NSE.log"


def get_nse_Bhav_file(date_today):
    str_date = date_today.strftime('%d%m%Y')
    ic(str_date)
    BHAV_FILE_NAME = 'sec_bhavdata_full_{}.csv'.format(str_date)
    # BHAV_FILE_NAME = 'sec_bhavdata_full_16112021.csv'
    if not os.path.exists(RAW_DIR + '\\' + BHAV_FILE_NAME):
        # downloading bhav file
        response = requests.get("https://archives.nseindia.com/products/content/{}".format(BHAV_FILE_NAME), timeout=2)
        open(RAW_DIR + '\\' + BHAV_FILE_NAME, 'wb').write(response.content)

    return BASE_DIR+'\\raw\\'+BHAV_FILE_NAME, str_date


def delivery_quant_clean(element):
    if element[' SERIES'] == ' BE':
        return element[' TTL_TRD_QNTY']
    elif element[' DELIV_QTY'].strip() == '-' :
        return 0
    else:
        return element[' DELIV_QTY']


def pChange(element):
    try:
        return int((float(element[' CLOSE_PRICE'])-float(element[' OPEN_PRICE']))/(float(element[' OPEN_PRICE']))*100)
    except:
        return 0


def filter_data(bhav_copy):
    return bhav_copy[bhav_copy[' SERIES'].isin([' BE', ' EQ', ' SM'])]


def price_band():
    dls = "https://archives.nseindia.com/content/equities/sec_list.csv"
    urllib.request.urlretrieve(dls, METADATA_DIR + "\\sec_list.csv")


# cm_bhav_file is used to get ISIN and called from get_stockmetadata.py
def get_cm_bhav_file(date_today):
    str_date = date_today.strftime('%d%m%Y')

    mon_str = date_today.strftime('%b').upper()
    year_str = str_date[-4:]
    day_str = str_date[:2]

    # https://archives.nseindia.com/content/historical/EQUITIES/2021/JUL/cm30JUL2021bhav.csv.zip
    print("https://archives.nseindia.com/content/historical/EQUITIES/" + str(year_str) +
          "/" + str(mon_str) + "/cm" + str(day_str) + str(mon_str) + str(year_str) + "bhav.csv.zip")
    response = requests.get(f"https://archives.nseindia.com/content/historical/EQUITIES/"+ str(year_str) +
                            "/"+ str(mon_str) +"/cm"+ str(day_str) + str(mon_str) +str(year_str) +"bhav.csv.zip", timeout=10)

    open(RAW_DIR + '\\nseoi.zip', 'wb').write(response.content)
    zf = ZipFile(RAW_DIR + "\\nseoi.zip")
    zf.extractall(RAW_DIR)
    zf.close()
    cm_bhav_file_csv = pd.read_csv(RAW_DIR + '\\cm'+ str(day_str) + str(mon_str) +str(year_str) +'bhav.csv')
    cm_bhav_file_csv['TIMESTAMP'] = pd.to_datetime(cm_bhav_file_csv['TIMESTAMP'], format='%d-%b-%Y')
    file_name = 'cm'+ str(day_str) + str(mon_str) +str(year_str) +'bhav.csv'

    return cm_bhav_file_csv, file_name


def update_nse_master_file(date_today):
    bhav_data_nse_master_file="bhav_data_nse_historical.csv"

    bhav_file_path,str_date = get_nse_Bhav_file(date_today)

    bhav_copy = pd.read_csv(bhav_file_path)
    bhav_copy[' DATE1'] = pd.to_datetime(bhav_copy[' DATE1'],format=' %d-%b-%Y')
    bhav_copy[' DATE1'] = bhav_copy[' DATE1'].dt.strftime('%d-%m-%Y %H:%M')  # .apply(date_correction)

    bhav_copy=filter_data(bhav_copy)

    bhav_copy[' DELIV_QTY'] = bhav_copy.apply(delivery_quant_clean, axis=1)
    bhav_copy['pChange'] = bhav_copy.apply(pChange, axis=1)

    bhav_copy.rename(columns={'SYMBOL': 'Stock', ' AVG_PRICE': 'vwap', ' NO_OF_TRADES': 'Trades',
                              ' DATE1': 'secWiseDelPosDate', ' CLOSE_PRICE': 'lastPrice',
                              ' DELIV_PER': 'deliveryToTradedQuantity',
                              ' DELIV_QTY': 'deliveryQuantity', ' TTL_TRD_QNTY': 'quantityTraded', ' HIGH_PRICE':'HighPrice',
                              ' OPEN_PRICE': 'OpenPrice', ' LOW_PRICE': 'Low'}, inplace = True)


    # os.chdir(BASE_DIR + '/processed/')
    bhav_copy.drop(
        [' PREV_CLOSE', ' LAST_PRICE', ' TURNOVER_LACS'],
        axis=1, inplace=True)


    try:
        if not os.path.exists(PROCESSED_DIR + '\\' + bhav_data_nse_master_file):
            bhav_copy.to_csv(PROCESSED_DIR + '\\' + bhav_data_nse_master_file, index=False)
        else:
            hist_df = pd.read_csv(PROCESSED_DIR + '\\' + bhav_data_nse_master_file)
            hist_df['secWiseDelPosDate'] = pd.to_datetime(hist_df['secWiseDelPosDate'], format='%d-%m-%Y %H:%M')
            bhav_copy['secWiseDelPosDate'] = pd.to_datetime(bhav_copy['secWiseDelPosDate'], format='%d-%m-%Y %H:%M')
            if bhav_copy['secWiseDelPosDate'].max() != hist_df['secWiseDelPosDate'].max():
                Historical = pd.concat([bhav_copy, hist_df])
                Historical['secWiseDelPosDate'] = Historical['secWiseDelPosDate'].dt.strftime('%d-%m-%Y %H:%M')
                Historical.to_csv(PROCESSED_DIR + '\\' + bhav_data_nse_master_file, index=False)

            # if not(datetime.datetime.today().date() == pd.to_datetime(bhav_data_nse_master_file_dataframe['secWiseDelPosDate']).max().date()):
            #     print('dikhna hi mat ye toh')
            #     pd.concat([bhav_copy, bhav_data_nse_master_file_dataframe]).to_csv(PROCESSED_DIR + '\\' + bhav_data_nse_master_file, index=False)

    except Exception as e:
            print (LOG_FILE_NAME)
            LOG_insert(LOG_FILE_NAME, str(e) + " Got error while updating master file", logging.ERROR)

    deleting_csv_files(bhav_file_path,LOG_FILE_NAME)
    return


def get_historical_file(HISTORICAL_DATA=0):
    if HISTORICAL_DATA == 1:
        start_date = datetime.date(day=1, month=1, year=2021)
        end_date = datetime.date(day=16, month=12, year=2021)
        totaldays = (end_date - start_date).days
        for i in range(totaldays + 1):

            date = start_date + datetime.timedelta(days=i)
            try:
                print(date)
                update_nse_master_file(date)
            except Exception as e:
                print(e, 'No data found for date')
                continue
    else:
        update_nse_master_file(date_today)
        price_band()


def avg_thread(stock, final_df):
    try:
        df_stock_perline = final_df[final_df['Stock'] == stock]
        df_stock_perline['SMA_20'] = df_stock_perline['lastPrice'].rolling(20, min_periods=0).mean()
        avg_all.append(df_stock_perline)
    except:
        pass


if __name__ == '__main__':
    try:
        HISTORICAL_DATA = 0
        get_historical_file(HISTORICAL_DATA)
        e = 'pass'
        execution_status(str(e), file, logging.ERROR, date_today, 1)
    except Exception as e:
        print(e)
        execution_status(str(e), file, logging.ERROR, date_today, 0)
        sendmail_err(file, e)

    a = pd.read_csv(PROCESSED_DIR + '\\bhav_data_nse_historical.csv')
    a['secWiseDelPosDate'] = pd.to_datetime(a['secWiseDelPosDate'], format='%d-%m-%Y %H:%M')
    a.sort_values(by=['secWiseDelPosDate'], inplace=True)
    uniqueValues = a['Stock'].unique()
    # uniqueValues = ['20MICRONS', '3IINFOTECH', '63MOONS', 'ADANIENT', 'ADANIPORTS']
    # ic(a['Stock'].unique())
    # exit()
    for stock in uniqueValues:
        ic(stock)
        t1 = threading.Thread(target=avg_thread, args=(stock, a))
        t1.start()
        threads.append(t1)

    for process in threads:
        process.join()
    df_stock = pd.concat(avg_all)

    df_stock.to_csv(METADATA_DIR + '\\moving_avg_nse.csv', index=None)











