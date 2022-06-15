import threading
import requests
import json
from get_bhav_file_BSE import update_master_file
from utils import *
from time import sleep

threads = []
delivery_position_all_list = []

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; rv:82.0) Gecko/20100101 Firefox/82.0"}


def load_bhav_copy(bhav_file_path):
    bhav_copy = pd.read_csv(bhav_file_path)
    bhav_copy = bhav_copy[~bhav_copy['SC_CODE'].isnull()]
    return bhav_copy


def start_thread(element):
    s = requests.session()
    try:
        print(element)
        url = "https://api.bseindia.com/BseIndiaAPI/api/SecurityPosition/w?quotetype=EQ&scripcode={}".format(
            int(element))
        response = s.get(url, headers=headers).json()

        url = "https://api.bseindia.com/BseIndiaAPI/api/StockTrading/w?flag=&quotetype=EQ&scripcode={}".format(
            int(element))
        r = s.get(url, headers=headers).json()

        url = "https://api.bseindia.com/BseIndiaAPI/api/ComHeader/w?quotetype=EQ&scripcode={}&seriesid=".format(
            int(element))
        res = s.get(url, headers=headers).json()

        url = "https://api.bseindia.com/BseIndiaAPI/api/getScripHeaderData/w?Debtflag=&scripcode={}&seriesid=".format(
            int(element))
        p = s.get(url, headers=headers).json()

        meta_trade_df = json.loads(response)

        meta_trade_df["TradeDate"] = pd.to_datetime(meta_trade_df["TradeDate"], format='%d %b %Y |%H:%M')
        meta_trade_df["QtyTraded"] = int("".join(meta_trade_df["QtyTraded"].split(",")))
        meta_trade_df["DeliverableQty"] = int("".join(meta_trade_df["DeliverableQty"].split(",")))
        meta_trade_df["PcDQ_TQ"] = float(meta_trade_df["PcDQ_TQ"])

        meta_trade_df['vwap'] = r['WAP']
        meta_trade_df['Series'] = res['Group']
        meta_trade_df['SC_NAME'] = res['SecurityId']
        meta_trade_df['SC_CODE'] = res['SecurityCode']
        meta_trade_df['lastPrice'] = p['CurrRate']['LTP']
        meta_trade_df['pChange'] = p['CurrRate']['PcChg']

        meta_trade_df = meta_trade_df.copy()

        delivery_position_all_list.append(meta_trade_df)

    except Exception as e:
        print(element, "is wrong, error", e)


def scrap_bse_block_deals_data():
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=browser_profile())
    driver.get("https://www.bseindia.com/markets/equity/eqreports/block_deals.aspx")
    pd.set_option('display.max_columns', None)

    rows = len(driver.find_elements_by_xpath(
        '/html/body/form/div[4]/div[1]/div/div/div[3]/div/div[3]/table/tbody/tr/td/div/table/tbody/tr'))

    cols = len(driver.find_elements_by_xpath(
        '/html/body/form/div[4]/div[1]/div/div/div[3]/div/div[3]/table/tbody/tr/td/div/table/tbody/tr[2]/td'))

    table_df = []
    # Printing the data of the table
    for r in range(2, rows + 1):  #
        for p in range(1, cols + 1):
            # obtaining the text from each column of the table
            value = driver.find_element_by_xpath \
                ('/html/body/form/div[4]/div[1]/div/div/div[3]/div/div[3]/table/tbody/tr/td/div/table/tbody/tr'
                 '[' + str(r) + ']/td[' + str(p) + ']').text
            print(value)
            table_df.append(value)
            table_df_csv = pd.DataFrame(table_df)
        print()

    table_df_csv = pd.DataFrame(table_df_csv.values.reshape(-1, 7),
                                columns=['secWiseDelPosDate', 'SC_CODE', 'Stock', 'Client_Name', 'Series',
                                         'quantityTraded', 'lastPrice'])
    table_df_csv['secWiseDelPosDate'] = pd.to_datetime(table_df_csv['secWiseDelPosDate'], format='%d/%m/%Y')
    driver.close()

    table_df_csv.drop(['Client_Name'], inplace=True, axis=1)
    table_df_csv = table_df_csv[table_df_csv['Series'] == 'B']
    table_df_csv['quantityTraded'] = table_df_csv['quantityTraded'].str.replace(',', '').astype(int)
    table_df_csv['lastPrice'] = table_df_csv['lastPrice'].str.replace(',', '').astype(float)

    table_df_csv = table_df_csv.groupby(['SC_CODE', 'Stock']) \
        .agg({'quantityTraded': ['sum'], 'lastPrice': ['first'], 'secWiseDelPosDate': ['first'],
              'Series': ['first']}).reset_index()
    table_df_csv.columns = ['SC_CODE', 'Stock', 'quantityTraded', 'lastPrice', 'secWiseDelPosDate', 'Series']

    table_df_csv['vwap'] = table_df_csv['lastPrice']
    table_df_csv['deliveryToTradedQuantity'] = 0
    table_df_csv['pChange'] = 0
    table_df_csv['deliveryQuantity'] = table_df_csv['quantityTraded']
    table_df_csv = table_df_csv[
        ['Stock', 'secWiseDelPosDate', 'lastPrice', 'quantityTraded', 'deliveryQuantity', 'deliveryToTradedQuantity'
            , 'pChange', 'vwap', 'Series', 'SC_CODE']]

    return table_df_csv


def main():
    date_today = datetime.date.today() #- datetime.timedelta(days=1)
    date_today, prev_date, fut_date = get_market_prev_date_fut_date(date_today)
    str_date = date_today.strftime('%d%m%y')

    if os.path.exists(RAW_DIR + '\\EQ_ISINCODE_' + prev_date.strftime('%d%m%y' + '.CSV')):
        bhav_copy = pd.read_csv(RAW_DIR + '\\EQ_ISINCODE_' + prev_date.strftime('%d%m%y' + '.CSV'))
    else:
        bhav_file_path, dat = update_master_file(prev_date)
        bhav_copy = load_bhav_copy(bhav_file_path)

    data = bhav_copy[['SC_CODE']].values.tolist()
    index = 0
    for Symbol in data:
        t1 = threading.Thread(target=start_thread, args=Symbol)
        t1.start()
        index = index + 1
        threads.append(t1)
        if index % 100 == 0:
            sleep(10)

    for process in threads:
        process.join()

    result_df = pd.DataFrame(delivery_position_all_list)
    result_df = result_df.rename(columns={"TradeDate": "secWiseDelPosDate", "QtyTraded": "quantityTraded",
                                          "DeliverableQty": "deliveryQuantity", "PcDQ_TQ": "deliveryToTradedQuantity",
                                          "SC_NAME": "Stock"})

    result_df = result_df[
        ["Stock", "secWiseDelPosDate", "lastPrice", "quantityTraded", "deliveryQuantity", "deliveryToTradedQuantity",
         "pChange", "vwap", "Series", "SC_CODE"]]
    result_df['secWiseDelPosDate'] = pd.to_datetime(result_df['secWiseDelPosDate'], format='%d-%m-%Y %H:%M:%S')

    if not os.path.exists(PROCESSED_DIR + '\\del_data_BSE' + '\\bse_delivery_data_' + str_date + '.csv'):
        result_df['filter_date'] = result_df['secWiseDelPosDate'].dt.strftime('%d-%m-%Y')
        result_df['secWiseDelPosDate'] = result_df['secWiseDelPosDate'].dt.strftime('%d-%m-%Y %H:%M')
        result_df['filter_date'] = pd.to_datetime(result_df['filter_date'], format='%d-%m-%Y')
        result_df = result_df[result_df['filter_date'] == result_df['filter_date'].max()]
        result_df.drop(['filter_date'], inplace=True, axis=1)
        result_df.to_csv(PROCESSED_DIR + '\\del_data_BSE' + '\\bse_delivery_data_' + str_date + '.csv', index=False)
    else:
        Historical = pd.read_csv(PROCESSED_DIR + '\\del_data_BSE' + '\\bse_delivery_data_' + str_date + '.csv')
        Historical['secWiseDelPosDate'] = pd.to_datetime(Historical['secWiseDelPosDate'], format='%d-%m-%Y %H:%M')
        result_df['filter_date'] = result_df['secWiseDelPosDate'].dt.strftime('%d-%m-%Y')
        result_df['filter_date'] = pd.to_datetime(result_df['filter_date'], format='%d-%m-%Y')
        result_df = result_df[result_df['filter_date'] == result_df['filter_date'].max()]
        result_df.drop(['filter_date'], inplace=True, axis=1)
        if result_df['secWiseDelPosDate'].max() != Historical['secWiseDelPosDate'].max():
            print('yy')
            Historical = pd.concat([result_df, Historical])
            Historical['secWiseDelPosDate'] = Historical['secWiseDelPosDate'].dt.strftime('%d-%m-%Y %H:%M')
            Historical.to_csv(PROCESSED_DIR + '\\del_data_BSE' + '\\bse_delivery_data_' + str_date + '.csv', index=False)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('error',e)
        file = os.path.basename(__file__)
        sendmail_err(file, e)
    # a = scrap_bse_block_deals_data()
    # ic(a)
