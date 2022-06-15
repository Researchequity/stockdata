import requests
from time import sleep
import threading
from get_BHAV_FILE_NSE import get_nse_Bhav_file, get_cm_bhav_file
from utils import *

file = os.path.basename(__file__)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
trial = 0
headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; rv:82.0) Gecko/20100101 Firefox/82.0"}

threads = []
meta_trade_bse_all = []
meta_trade_nse_all = []


def make_dataframe(s, Symbol):
    stock_metadata_df = pd.DataFrame()
    print(Symbol)
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 6.1; rv:82.0) Gecko/20100101 Firefox/82.0"}
        r_stock = s.get('https://www.nseindia.com/api/quote-equity?symbol=' + Symbol.replace('&', '%26'),
                        headers=headers).json()
        r_mcp = s.get(
            'https://www.nseindia.com/api/quote-equity?symbol=' + Symbol.replace('&', '%26') + '&section=trade_info',
            headers=headers).json()
        tag = None
        if r_mcp['marketDeptOrderBook']['tradeInfo']['totalMarketCap'] / 100 <= 1000:
            tag = "VSM"
        elif 1000 < r_mcp['marketDeptOrderBook']['tradeInfo']['totalMarketCap'] / 100 <= 7000:
            tag = "Small"
        elif 7000 < r_mcp['marketDeptOrderBook']['tradeInfo']['totalMarketCap'] / 100 <= 28000:
            tag = "Mid"
        elif 28000 < r_mcp['marketDeptOrderBook']['tradeInfo']['totalMarketCap'] / 100:
            tag = "LARGE"

        stock_metadata_df['Symbol'] = [Symbol]
        stock_metadata_df['MarketCap'] = [tag]
        stock_metadata_df['companyName'] = pd.DataFrame([r_stock['info']['companyName']])
        stock_metadata_df['pdSectorInd'] = [r_stock['metadata']['pdSectorInd'].rstrip()]
        # stock_metadata_df['Industry'] = [r_stock['metadata']['industry'].rstrip()]
        stock_metadata_df['totalMarketCap'] = [r_mcp['marketDeptOrderBook']['tradeInfo']['totalMarketCap']]
        stock_metadata_df['issued_shares'] = [r_stock['securityInfo']['issuedSize']]
        stock_metadata_df['surveillance'] = [r_stock['securityInfo']['surveillance']]
        stock_metadata_df['Industry'] = [r_stock['industryInfo']['industry']]
        stock_metadata_df['basicIndustry'] = [r_stock['industryInfo']['basicIndustry']]
        stock_metadata_df['macro'] = [r_stock['industryInfo']['macro']]
        stock_metadata_df['Sector_new'] = [r_stock['industryInfo']['sector']]
    except:
        print('error in nse symbol')
        s = requests.session()
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36"}
        s.get('https://www.nseindia.com/get-quotes/derivatives?symbol=' + 'NIFTY', headers=headers)

    meta_trade_nse_all.append(stock_metadata_df)


def bse_get_data(element, s):
    try:
        meta_trade_df = pd.DataFrame()
        print(element)

        url = "https://api.bseindia.com/BseIndiaAPI/api/ComHeader/w?quotetype=EQ&scripcode={}&seriesid=".format(
            int(element))
        res = (s.get(url, headers=headers).json())
        url = "https://api.bseindia.com/BseIndiaAPI/api/StockTrading/w?flag=&quotetype=EQ&scripcode={}".format(int(element))
        mcap = s.get(url, headers=headers).json()
        url = "https://api.bseindia.com/BseIndiaAPI/api/getScripHeaderData/w?Debtflag=&scripcode={}&seriesid=".format(int(element))
        name = s.get(url, headers=headers).json()

        tag = None
        MktCapFull = mcap['MktCapFull']
        MktCapFull = MktCapFull.replace(',', '')
        try:
            if float(MktCapFull) <= 1000:
                tag = "VSM"
            elif 1000 < float(MktCapFull) <= 7000:
                tag = "Small"
            elif 7000 < float(MktCapFull) <= 28000:
                tag = "Mid"
            elif 28000 < float(MktCapFull):
                tag = "LARGE"
        except:
            pass

        meta_trade_df['ISIN'] = [res['ISIN']]
        meta_trade_df['script_code'] = [res['SecurityCode']]
        meta_trade_df['Sector'] = [res['Sector']]
        meta_trade_df['companyName'] = [name['Cmpname']['FullN']]
        meta_trade_df['Industry'] = [res['Industry']]
        meta_trade_df['pdSectorInd'] = [res['Grp_Index']]
        meta_trade_df['Series'] = [res['Group']]
        meta_trade_df['totalMarketCap'] = [mcap['MktCapFull']]
        meta_trade_df['totalMarketCap'] = meta_trade_df['totalMarketCap'].str.replace(',', '')
        meta_trade_df['totalMarketCap'] = meta_trade_df['totalMarketCap'].str.replace('-', '0')
        meta_trade_df['MarketCap'] = [tag]
        meta_trade_df['issued_shares'] = (
        [float(meta_trade_df['totalMarketCap']) * 10000000 / float(name['CurrRate']['LTP'])])  #
        meta_trade_df['Date_Updated'] = datetime.date.today()

        meta_trade_bse_all.append(meta_trade_df)
    except:
        sleep(2)
    # except Exception as e:
    #     print(element + 'err' + e)


def get_metadata(Symbol_latest_df):
    # for nse stock details
    Symbol_latest_nse = Symbol_latest_df[Symbol_latest_df['Symbol'].notnull()]
    if trial==1:
        Symbol_latest_nse = Symbol_latest_nse.head(20)

    Symbol_list = Symbol_latest_nse['Symbol'].astype(str).values.tolist()

    s = requests.session()
    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
                             "Chrome/85.0.4183.102 Safari/537.36"}
    s.get('https://www.nseindia.com/get-quotes/derivatives?symbol=' + 'NIFTY', headers=headers)

    index = 0
    for Symbol in Symbol_list:
        t1 = threading.Thread(target=make_dataframe, args=[s, Symbol])
        t1.start()
        index = index + 1
        threads.append(t1)
        if index % 30 == 0:
            sleep(1)
        if index % 200 == 0:
            s = requests.session()
            headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
                                     "Chrome/85.0.4183.102 Safari/537.36"}
            s.get('https://www.nseindia.com/get-quotes/derivatives?symbol=' + 'NIFTY', headers=headers)

    for process in threads:
        process.join()

    stock_metadata_nse_df = pd.concat(meta_trade_nse_all)

    # for bse stock details
    Symbol_latest_bse = Symbol_latest_df[~Symbol_latest_df['Symbol'].notnull()]

    # we have total of approx 1754
    if trial ==1:
        Symbol_latest_bse = Symbol_latest_bse.head(20)
    stock_code = Symbol_latest_bse['script_code'].astype(str).values.tolist()

    index = 0
    s = requests.session()
    for element in stock_code:
        t1 = threading.Thread(target=bse_get_data, args=[element, s])
        t1.start()
        index = index + 1
        threads.append(t1)
        if index % 30 == 0:
            sleep(1)

        if index % 200 == 0:
            s = requests.session()

    for process in threads:
        process.join()

    stock_metadata_bse_df = pd.concat(meta_trade_bse_all)


    return stock_metadata_nse_df, stock_metadata_bse_df


if __name__ == '__main__':
    date_today = datetime.date.today() #- datetime.timedelta(days=1)
    date_today, prev_date, fut_date = get_market_prev_date_fut_date(date_today)

    # Getting path of Project Directory
    # Getting path of holiday csv file
    # Downloading bhav file and returning its absolute path
    try:
        BHAV_DATA, date_str = get_nse_Bhav_file(date_today)
        ic(BHAV_DATA)
        cm_bhav_file_csv, file_name = get_cm_bhav_file(date_today)
        # cm_bhav_file_csv = r'\\192.168.41.190\program\stockdata\raw\cm04JAN2022bhav.csv'
        # file_name = 'cm04JAN2022bhav.csv'
        ic(file_name)

        cm_bhav_file = pd.read_csv(RAW_DIR + '\\' + file_name, usecols=['SYMBOL', 'ISIN', 'SERIES'])
        # os.remove(data +'\\'+ file_name)
        cm_bhav_file = cm_bhav_file[cm_bhav_file['SERIES'].isin(['BE', 'EQ', 'SM'])]
        cm_bhav_file.drop(['SERIES'], inplace=True, axis=1)
        Symbol_latest_df = pd.read_csv(BHAV_DATA)[['SYMBOL', ' DATE1', ' SERIES']] #, usecols=['SYMBOL', ' DATE1', ' SERIES'])

        Symbol_latest_df = Symbol_latest_df[Symbol_latest_df[' SERIES'].isin([' BE', ' EQ', ' SM'])]
        Symbol_latest_df = Symbol_latest_df[~Symbol_latest_df['SYMBOL'].str.contains('ETF')]
        Symbol_latest_df = pd.merge(Symbol_latest_df, cm_bhav_file, on=['SYMBOL'], how='left')
        Symbol_latest_df.rename(columns={"SYMBOL": "Symbol", " DATE1": "Date_Updated", "SERIES": "Series"}, inplace=True)

        symbol_ISIN = Symbol_latest_df['ISIN'].tolist()
        bse_master_script_code_df = pd.read_csv(METADATA_DIR + '\\bse_master_script_code.csv')[["SC_NAME", "SC_CODE", "ISIN_CODE"]]
        bse_master_script_code_df = bse_master_script_code_df.rename(
            columns={"SC_NAME": "companyName", "SC_CODE": "script_code", "ISIN_CODE": "ISIN"})

        New_ISIN = bse_master_script_code_df[~bse_master_script_code_df['ISIN'].isin(symbol_ISIN)]
        New_ISIN = pd.DataFrame(New_ISIN)
        Symbol_latest_df = pd.merge(Symbol_latest_df, bse_master_script_code_df, on=['ISIN'], how='left')
        Symbol_latest_df = pd.concat([Symbol_latest_df, New_ISIN])
        Symbol_latest_df['script_code'] = Symbol_latest_df['script_code'].astype('Int64')


        # Getting SYMBOL and DATE  from bhav data file
        token_df = pd.read_csv(METADATA_DIR + '\\Token_security.csv', usecols=['token', 'Symbol'])

        # Getting SYMBOL,Sector and Industry  from NSE DELIVERY file
        #sect_ind_df = pd.read_csv(METADATA_DIR + '\\NSE_Sector_Industry.csv', usecols=['Symbol', 'CD_Sector'])  # , 'CD_Industry1'
        #sect_ind_df.rename(columns={"CD_Sector": "Sector"}, inplace=True)  # , "CD_Industry1": "Industry"
        stock_metadata_all_df, stock_metadata_bse_df = get_metadata(Symbol_latest_df)
        Symbol_latest_df.drop(['companyName'], inplace=True, axis=1)
        stock_metadata_all_df = stock_metadata_all_df.merge(Symbol_latest_df, on='Symbol', how='inner')
        stock_metadata_all_df.drop([' SERIES'], inplace=True, axis=1)
        #stock_metadata_all_df = stock_metadata_all_df.merge(sect_ind_df, on='Symbol', how='left')
        stock_metadata_all_df['Date_Updated'] = pd.to_datetime(stock_metadata_all_df['Date_Updated'], format='  %d-%b-%Y')
        stock_metadata_all_df['Date_Updated'] = stock_metadata_all_df['Date_Updated'].dt.strftime('%d-%m-%Y')
        stock_metadata_all_df = pd.concat([stock_metadata_all_df, stock_metadata_bse_df])
        stock_metadata_all_df = stock_metadata_all_df.merge(token_df, on='Symbol', how='left')
        stock_metadata_all_df["Sector_new"].fillna(stock_metadata_all_df['Sector'], inplace=True)
        stock_metadata_all_df.drop(['Sector'], inplace=True, axis=1)
        stock_metadata_all_df = stock_metadata_all_df.rename(columns={"Sector_new": "Sector"})
        stock_metadata_all_df.to_csv(METADATA_DIR + '\\StockMetadata.csv', index=None)

        execution_status('pass', file, logging.ERROR, date_today, 1)

    except Exception as e:
        print(e)
        execution_status(str(e), file, logging.ERROR, date_today, 0)
        sendmail_err(file, e)

