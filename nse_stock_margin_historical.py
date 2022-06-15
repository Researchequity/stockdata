from utils import *
import urllib
import socket

socket.setdefaulttimeout(2)
today_date = datetime.date.today() #- datetime.timedelta(days=1)
file = os.path.basename(__file__)

def nse_stock_margin(dat):
    date_today = dat.strftime('%d%m%Y')
    ic(date_today)
    try:
        url = 'https://archives.nseindia.com/archives/nsccl/var/C_VAR1_'+ str(date_today) +'_3.DAT'
        urllib.request.urlretrieve(url, RAW_DIR + "\\C_VAR1_" + str(date_today) + "_3.DAT")
        temp_df = pd.read_csv(RAW_DIR + "\\C_VAR1_" + date_today + "_3.DAT")

        temp_df = pd.DataFrame(temp_df).reset_index()
        temp_df.columns = ['0', 'symbol', 'series', 'isin', 'Securit VaR', 'Index VaR', 'VaR Margin',
                           'Extreme Loss Rate', 'Adhoc Margin', 'Applicable Margin Rate']
        temp_df['date'] = date_today
        temp_df.drop(['0'], inplace=True, axis=1)
        temp_df = temp_df[temp_df['series'].isin(['BE', 'EQ', 'SM'])]

        temp_df['date'] = pd.to_datetime(temp_df['date'], format='%d%m%Y')
        temp_df = temp_df[['symbol', 'series', 'isin', 'Securit VaR', 'Index VaR', 'VaR Margin',
                    'Extreme Loss Rate', 'Adhoc Margin', 'Applicable Margin Rate', 'date']]


        if not os.path.exists(PROCESSED_DIR + '\\nse_stock_margin_historical.csv'):
            temp_df['date'] = temp_df['date'].dt.strftime('%d-%m-%Y')
            temp_df.to_csv(PROCESSED_DIR + '\\nse_stock_margin_historical.csv', index=False)
        else:
            historical_file = pd.read_csv(PROCESSED_DIR + '\\nse_stock_margin_historical.csv')
            historical_file['date'] = pd.to_datetime(historical_file['date'], format='%d-%m-%Y')

            if historical_file['date'].max() != temp_df['date'].max():

                Historical = pd.concat([historical_file, temp_df])
                Historical['date'] = Historical['date'].dt.strftime('%d-%m-%Y')
                Historical.to_csv(PROCESSED_DIR + '\\nse_stock_margin_historical.csv', index=False)
    except Exception as e:
        print(e)
        print('err on date :' + str(dat) + e)


def get_historical_bhav_file(HISTORICAL_DATA=0):
    if HISTORICAL_DATA == 1:
        start_date = datetime.date(day=1, month=8, year=2021)
        end_date = datetime.date(day=31, month=12, year=2021)
        totaldays = (end_date - start_date).days
        for i in range(totaldays + 1):

            date = start_date + datetime.timedelta(days=i)
            try:
                print(date)
                nse_stock_margin(date)
            except Exception as e:
                print(e, 'No data found for date')
                continue
    else:
        try:
            nse_stock_margin(today_date)
            execution_status('pass', file, logging.ERROR, today_date, 1)
        except Exception as e:
            print(e)
            execution_status(str(e), file, logging.ERROR, today_date, 0)
            sendmail_err(file, e)


if __name__ == '__main__':
    HISTORICAL_DATA = 0
    get_historical_bhav_file(HISTORICAL_DATA)

    # temp = temp[temp.symbol == '20MICRONS']
    # temp.to_csv(r'\\192.168.41.190\shared\\EXPLEOSOL.csv')





