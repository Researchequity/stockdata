from utils import *
import requests

today_date = datetime.datetime.today() #- datetime.timedelta(days=1)


def download_file(dat):
    date_today = dat
    year_date = dat.strftime('%Y')
    dat = dat.strftime('%d%m')

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br"
    }

    s = requests.session()
    url = 'https://www.bseindia.com/BSEDATA/gross/' + year_date + '/SCBSEALL' + dat + '.zip'
    response = s.get(url, headers=headers)
    open(RAW_DIR + '\\SCBSEALL', 'wb').write(response.content)
    zf = ZipFile(RAW_DIR + "\\SCBSEALL")
    zf.extractall(RAW_DIR)
    zf.close()
    SCBSEALL = pd.read_csv(RAW_DIR + '\\SCBSEALL' + dat + '.txt', delimiter="|")
    SCBSEALL = pd.DataFrame(SCBSEALL)
    SCBSEALL.drop(['DATE'], inplace=True, axis=1)
    SCBSEALL['DATE'] = date_today
    SCBSEALL['DATE'] = pd.to_datetime(SCBSEALL['DATE'], format='%Y-%m-%d')

    if not os.path.exists(PROCESSED_DIR + '\\SCBSEALL_historical.csv'):
        SCBSEALL['DATE'] = SCBSEALL['DATE'].dt.strftime('%d-%m-%Y')
        SCBSEALL.to_csv(PROCESSED_DIR + '\\SCBSEALL_historical.csv', index=False)
    else:
        Historical = pd.read_csv(PROCESSED_DIR + '\\SCBSEALL_historical.csv')
        Historical['DATE'] = pd.to_datetime(Historical['DATE'], format='%d-%m-%Y')
        if SCBSEALL['DATE'].max() != Historical['DATE'].max():
            Historical = pd.concat([SCBSEALL, Historical])
            Historical['DATE'] = Historical['DATE'].dt.strftime('%d-%m-%Y')
            Historical.to_csv(PROCESSED_DIR + '\\SCBSEALL_historical.csv', index=False)


def get_historical_file(HISTORICAL_DATA=0):
    if HISTORICAL_DATA:
        start_date = datetime.date(day=6, month=2, year=2021)
        end_date = datetime.date(day=6, month=12, year=2021)
        totaldays = (end_date - start_date).days
        for i in range(totaldays + 1):

            date = start_date + datetime.timedelta(days=i)
            try:
                print(date)
                download_file(date)
            except Exception as e:
                print(e, 'No data found for date')
                continue
    else:
        download_file(today_date)


if __name__ == '__main__':
    HISTORICAL_DATA = 0
    get_historical_file(HISTORICAL_DATA)

    # Get average
    SCBSEALL_hist = pd.read_csv(PROCESSED_DIR + '\\SCBSEALL_historical.csv')
    SCBSEALL_hist['DATE'] = pd.to_datetime(SCBSEALL_hist['DATE'], format='%d-%m-%Y')
    SCBSEALL_hist['avg_stock'] = SCBSEALL_hist['SCRIP CODE']
    SCBSEALL_hist['avg_date'] = SCBSEALL_hist['DATE']
    SCBSEALL_hist['avg_col'] = SCBSEALL_hist['DELIVERY QTY']
    SCBSEALL_hist.sort_values(by=['avg_date'], inplace=True)  # , ascending=False
    average_csv = average(SCBSEALL_hist, today_date)
    average_csv = average_csv[["SCRIP CODE", "avg_norm_mean", "DATE"]]
    average_csv = average_csv.rename(columns={"avg_norm_mean": "DEL_norm_mean"})
    average_csv.to_csv(METADATA_DIR + '\\Average_bse.csv', index=False)

    ## To get average historical use Template_Average_Hist.py code



