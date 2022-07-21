import requests
import dateutil
from utils import *
file = os.path.basename(__file__)

date_today = datetime.date.today() #- datetime.timedelta(days=1)
date_today, prev_date, fut_date = get_market_prev_date_fut_date(date_today)

LOG_FILE_NAME = str(LOG_DIR)+"\\get_BHAV_FILE_BSE.log"
str_date = date_today.strftime('%d%m%y')

# when called from delivery_data_BSE.py pev_date is used...


def get_Bhav_file(dat):
    str_date = dat.strftime('%d%m%y')
    headers={"User-Agent": "Mozilla/5.0 (Windows NT 6.1; rv:82.0) Gecko/20100101 Firefox/82.0"}

    BHAV_FILE_NAME = "EQ_ISINCODE_{}.zip".format(str_date)

    #BHAV_FILE_NAME = "EQ_ISINCODE_100321.zip"
    if not os.path.exists(BASE_DIR+'/raw/'+BHAV_FILE_NAME):

        # downloading bhav file
        response = requests.get("https://www.bseindia.com/download/BhavCopy/Equity/{}".format(BHAV_FILE_NAME), headers=headers)
        file = open(BASE_DIR+'/raw/'+BHAV_FILE_NAME, 'wb').write(response.content)
        #file.close()

    return BASE_DIR+'\\raw\\'+BHAV_FILE_NAME, str_date


def filter_bhav_copy(bhav_copy):
    bhav_copy = bhav_copy[bhav_copy['SC_GROUP'].apply(lambda element: element.strip()[0]).isin(['A', 'B', 'X', 'XT', 'T','M','MT'])]
    bhav_copy = bhav_copy[~bhav_copy['SC_NAME'].str.contains('ETF')]
    return bhav_copy


def update_master_file(dat):
    str_date = dat
    ic(str_date, 'master_file_date')
    master_script_code_file = "bse_master_script_code.csv"
    bhav_data_bse_master_file = "bhav_data_bse_historical.csv"
    bhav_file_path, str_date = get_Bhav_file(str_date)

    unzip_folder(bhav_file_path,LOG_FILE_NAME)
    bhav_file_path = bhav_file_path.split(".zip")[0]+".CSV"
    bhav_copy = pd.read_csv(bhav_file_path)
    bhav_copy = bhav_copy[bhav_copy['SC_TYPE'] == 'Q']
    bhav_copy.drop(['SC_TYPE', 'HIGH', 'LAST', 'LOW', 'NET_TURNOV', 'TDCLOINDI', 'FILLER2', 'FILLER3'],
                   axis=1, inplace=True)
    bhav_copy['TRADING_DATE'] = pd.to_datetime(bhav_copy['TRADING_DATE'], format='%d-%b-%y')
    try:
        if os.path.exists(PROCESSED_DIR + '\\' + bhav_data_bse_master_file):
            bhav_data_df = pd.read_csv(PROCESSED_DIR + '\\' + bhav_data_bse_master_file)
            bhav_data_df['TRADING_DATE'] = pd.to_datetime(bhav_data_df['TRADING_DATE'], format='%d-%m-%Y')
            if bhav_copy['TRADING_DATE'].max() != bhav_data_df['TRADING_DATE'].max():
                Historical = pd.concat([bhav_copy, bhav_data_df])
                Historical['TRADING_DATE'] = Historical['TRADING_DATE'].dt.strftime('%d-%m-%Y')
                Historical.to_csv(PROCESSED_DIR + '\\' + bhav_data_bse_master_file, index=False)
        else:
            bhav_copy['TRADING_DATE'] = bhav_copy['TRADING_DATE'].dt.strftime('%d-%m-%Y')
            bhav_copy.to_csv(PROCESSED_DIR + '\\' + bhav_data_bse_master_file, index=False)

        bhav_copy = filter_bhav_copy(bhav_copy)


        if not os.path.exists(METADATA_DIR + '\\' + master_script_code_file):
            bhav_copy[['SC_CODE', 'SC_NAME','ISIN_CODE', 'SC_GROUP']].to_csv(METADATA_DIR + '\\' + master_script_code_file, index=False)
        else:
            master_copy = pd.read_csv(METADATA_DIR + '\\' + master_script_code_file)
            master_copy.drop(master_copy[master_copy['SC_CODE'].isin(bhav_copy['SC_CODE'].unique())].index, inplace=True)
            bhav_copy = pd.concat([bhav_copy, master_copy])
            bhav_copy[['SC_CODE', 'SC_NAME','ISIN_CODE', 'SC_GROUP']].to_csv(METADATA_DIR + '\\' + master_script_code_file, index=False)
    except Exception as e:
            LOG_insert(LOG_FILE_NAME, str(e) + " Got error while updating master file", logging.ERROR)

    # deleting_zipped_folder(LOG_FILE_NAME)
    # deleting_csv_files(bhav_file_path, LOG_FILE_NAME)

    return bhav_file_path, str_date


def get_historical_file(HISTORICAL_DATA=0):
    if HISTORICAL_DATA == 1:
        start_date = datetime.date(day=1, month=1, year=2021)
        end_date = datetime.date(day=2, month=3, year=2022)
        totaldays = (end_date - start_date).days
        for i in range(totaldays + 1):

            date = start_date + datetime.timedelta(days=i)
            try:
                print(date)
                update_master_file(date)
            except Exception as e:
                print(e, 'No data found for date')
                continue
    else:
        update_master_file(date_today)



if __name__ == '__main__':
    try:
        HISTORICAL_DATA = 0
        get_historical_file(HISTORICAL_DATA)
        execution_status('pass', file, logging.ERROR, date_today, 1)
    except Exception as e:
        print(e)
        sendmail_err(file, e)
        execution_status(str(e), file, logging.ERROR, str_date, 0)
