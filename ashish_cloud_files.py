import urllib
from urllib.request import urlopen
import pandas as pd
import requests
from zipfile import ZipFile
from icecream import ic
from dateutil.relativedelta import relativedelta
import xlwings as xw
from utils import *
import socket
socket.setdefaulttimeout(5)

ashish_dir = r'\\192.168.41.190\ashish'


date_today = datetime.date.today() #- datetime.timedelta(days=1)
date_today, prev_date, fut_date = get_market_prev_date_fut_date(date_today)

today_date = date_today.strftime('%d%m%Y')
yest_date = prev_date.strftime('%d%m%Y')
next_date = fut_date.strftime('%d%m%Y')
fii_derivatives_date = date_today.strftime('%d-%b-%Y')


def bulk_nse():
    dls = "https://archives.nseindia.com/content/equities/bulk.csv"
    urllib.request.urlretrieve(dls, ashish_dir + "\\bulk_nse.csv")
    bulk_csv = pd.read_csv(ashish_dir + "\\bulk_nse.csv")
    os.remove(ashish_dir + "\\bulk_nse.csv")
    bulk_csv.drop(['Date', 'Security Name', 'Remarks'], inplace=True, axis=1)
    bulk_csv.sort_values(by=['Symbol', 'Client Name'], ascending=True, inplace=True)
    bulk_csv.to_csv(ashish_dir + "\\bulk_nse_" + today_date + ".csv", index=None)


def block_nse():
    dls = "https://archives.nseindia.com/content/equities/block.csv"
    urllib.request.urlretrieve(dls, ashish_dir + "\\block_nse_" + today_date + ".csv")
    block_nse_df = pd.read_csv(ashish_dir + "\\block_nse_" + today_date + ".csv")
    block_nse_df.drop(['Security Name'], inplace=True, axis=1)
    os.remove(ashish_dir + "\\block_nse_" + today_date + ".csv")
    block_nse_df.to_csv(ashish_dir + "\\block_nse_" + today_date + ".csv", index=None)


def fno_combine_OI():
    response = requests.get(f"https://archives.nseindia.com/archives/nsccl/mwpl/combineoi_" + today_date + ".zip", timeout=1)
    open(ashish_dir + '\\combineoi.zip', 'wb').write(response.content)
    zf = ZipFile(ashish_dir + "\\combineoi.zip")
    zf.extractall(ashish_dir)
    zf.close()
    os.remove(ashish_dir + "\\combineoi.zip")
    os.remove(ashish_dir + "\\combineoi_" + today_date + ".xml")

    combineoi = pd.read_csv(ashish_dir + "\\combineoi_" + today_date + ".csv")
    combineoi = combineoi[combineoi[' Limit for Next Day'] == 'No Fresh Positions']
    combineoi['p_chng'] = (combineoi[' Open Interest'] * 100) / combineoi[' MWPL']
    df_merge = combineoi[[" NSE Symbol", "p_chng"]]
    df_merge = df_merge.round({"p_chng": 0})
    df_merge.to_csv(ashish_dir + "\\combineOI_processed.csv", index=None)


def participant_wise_OI():
    dls = "https://archives.nseindia.com/content/nsccl/fao_participant_oi_" + today_date + ".csv"
    urllib.request.urlretrieve(dls, ashish_dir + "\\fao_participant_oi" + today_date + ".csv")
    today_df = pd.read_csv(ashish_dir + "\\fao_participant_oi" + today_date + ".csv", skiprows=1)
    today_df.drop(['Client Type'], inplace=True, axis=1)

    dls = "https://archives.nseindia.com/content/nsccl/fao_participant_oi_" + yest_date + ".csv"

    urllib.request.urlretrieve(dls, ashish_dir + "\\fao_participant_oi" + yest_date + ".csv")
    prev_df = pd.read_csv(ashish_dir + "\\fao_participant_oi" + yest_date + ".csv", skiprows=1)
    prev_df.drop(['Client Type'], inplace=True, axis=1)
    diff = today_df.subtract(prev_df, fill_value=0)


    today_df = pd.DataFrame(today_df)
    prev_df = pd.DataFrame(prev_df)
    diff = pd.DataFrame(diff)
    data_excel_file = ashish_dir + "\\OI_POSITION.xlsx"
    wb = xw.Book(data_excel_file)
    sheet_oi_single = wb.sheets('Input')
    sheet_oi_single.range("B2").options(index=None).value = today_df
    sheet_oi_single.range("M1").options(index=None).value = today_date
    sheet_oi_single.range("B10").options(index=False).value = diff
    wb.save()
    app = xw.apps.active
    app.quit()


def surveillance_investigation():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9", "Accept-Encoding": "gzip, deflate, br"}
    s = requests.session()
    s.get('https://www.nseindia.com/get-quotes/derivatives?symbol=' + 'NIFTY', headers=headers)
    r = s.get('https://www.nseindia.com/api/circulars', headers=headers).json()

    surv_df_all = pd.DataFrame()

    for i in range(len(r['data'])):
        temp_df = pd.DataFrame([r['data'][i]])
        temp_df = temp_df[
            ['fileDept', 'circNumber', 'fileExt', 'sub', 'cirDate', 'cirDisplayDate', 'circFilename',
             'circFilelink', 'circCompany', 'circDisplayNo', 'circCategory', 'circDepartment']]
        surv_df_all = pd.concat([surv_df_all, temp_df])

    surv_df_all['cirDate'] = pd.to_datetime(surv_df_all['cirDate'], format='%Y%m%d')
    # surv_df_all['cirDate'] = surv_df_all['cirDate'].dt.strftime('%d-%m-%Y')
    filt1 = surv_df_all[surv_df_all["sub"].isin(['Applicability of Additional Surveillance Measure (ASM)'])]
    filt1 = filt1[filt1['cirDate'] == filt1['cirDate'].max()]

    filt2 = surv_df_all[surv_df_all["sub"].isin(['Applicability of Short-Term Additional Surveillance Measure (ST-ASM)']
                                                )]
    filt2 = filt2[filt2['cirDate'] == filt2['cirDate'].max()]
    get_zip_link_one = filt1['circFilelink'][0]
    get_zip_link_two = filt2['circFilelink'][0]

    ic(get_zip_link_one)
    ic(get_zip_link_two)
    response = requests.get(get_zip_link_one, timeout=1)
    open(ashish_dir + '\\get_zip_link_one.zip', 'wb').write(response.content)
    zf = ZipFile(ashish_dir + "\\get_zip_link_one.zip")
    zf.extractall(ashish_dir)
    zf.close()
    response = requests.get(get_zip_link_two, timeout=1)
    open(ashish_dir + '\\get_zip_link_two.zip', 'wb').write(response.content)
    zf = ZipFile(ashish_dir + "\\get_zip_link_two.zip")
    zf.extractall(ashish_dir)
    zf.close()


def eq_band_change():

    dls = "https://archives.nseindia.com/content/equities/eq_band_changes_" + next_date + ".csv"
    urllib.request.urlretrieve(dls, ashish_dir + "\\eq_band_change.csv")
    eq_band_change = pd.read_csv(ashish_dir + "\\eq_band_change.csv")
    os.remove(ashish_dir + "\\eq_band_change.csv")
    eq_band_change.drop(['Security Name'], inplace=True, axis=1)
    eq_band_change.to_csv(ashish_dir + "\\eq_band_change.csv", index=None)


def fii_derivatives():
    dls = "https://archives.nseindia.com/content/fo/fii_stats_" + fii_derivatives_date + ".xls".format(fii_derivatives_date)
    urllib.request.urlretrieve(dls, ashish_dir + "\\fii_stats_" + fii_derivatives_date + ".xls".format(fii_derivatives_date))
    fii_stats = pd.read_excel(ashish_dir + "\\fii_stats_" + fii_derivatives_date + '.xls'.format(fii_derivatives_date), skiprows=2).fillna(0)
    os.remove(ashish_dir + "\\fii_stats_" + fii_derivatives_date + '.xls'.format(fii_derivatives_date))
    fii_stats = pd.DataFrame(fii_stats)
    fii_stats = fii_stats.loc[(fii_stats['No. of contracts'] != 0)].reset_index()
    fii_stats.columns = ['index', 'FUTURES', 'Buy_no_contract', 'Buy_amt_contract', 'Sell_no_contract',
                         'Sell_amt_contract', 'no_contract', 'amount']
    fii_stats['Val_Diff'] = fii_stats['Buy_amt_contract'] - fii_stats['Sell_amt_contract']
    fii_stats = fii_stats[['FUTURES', 'Val_Diff']]
    fii_stats.to_csv(ashish_dir + "\\fii_derivatives" + fii_derivatives_date +".csv", index=None)


def bulk_bse():

    r = requests.get("https://www.bseindia.com/markets/equity/eqreports/bulk_deals.aspx", headers=header)

    driver = webdriver.Chrome(ChromeDriverManager().install(), options=browser_profile())
    driver.get("https://www.bseindia.com/markets/equity/eqreports/bulk_deals.aspx")
    dfs = pd.read_html(r.text)
    pd.set_option('display.max_columns', None)

    rows = len(driver.find_elements_by_xpath(
        '/html/body/form/div[4]/div/div/div[3]/div/div/table/tbody/tr/td/div/table/tbody/tr'))

    cols = len(driver.find_elements_by_xpath(
        '/html/body/form/div[4]/div/div/div[3]/div/div/table/tbody/tr/td/div/table/tbody/tr[2]/td'))

    table_df = []
    # Printing the data of the table
    for r in range(2, rows + 1): #
        for p in range(1, cols + 1):
            # obtaining the text from each column of the table
            value = driver.find_element_by_xpath('/html/body/form/div[4]/div/div/div[3]/div/div/table/tbody/tr/td/div/table/tbody/tr['+ str(r) + ']/td[' + str(p) + ']').text
            print(value)
            table_df.append(value)
            table_df_csv = pd.DataFrame(table_df)
        print()

    table_df_csv = pd.DataFrame(table_df_csv.values.reshape(-1, 7),
                    columns=['Date','Symbol','Security Name', 'Client Name',
                             'Buy/Sell', 'Quantity Traded', 'Trade Price / Wght. Avg. Price'])
    table_df_csv.drop(['Date','Symbol'], inplace=True, axis=1)
    # table_df_csv['Date'] = pd.to_datetime(table_df_csv['Date'],format='%d/%m/%Y')
    driver.close()
    table_df_csv.to_csv(ashish_dir + "\\bulk_bse.csv", index=None)


def block_bse():

    r = requests.get("https://www.bseindia.com/markets/equity/eqreports/block_deals.aspx", headers=header)

    driver = webdriver.Chrome(ChromeDriverManager().install(), options=browser_profile())
    driver.get("https://www.bseindia.com/markets/equity/eqreports/block_deals.aspx")
    dfs = pd.read_html(r.text)
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
            value = driver.find_element_by_xpath('/html/body/form/div[4]/div[1]/div/div/div[3]/div/div[3]/table/tbody/tr/td/div/table/tbody/tr['+ str(r) + ']/td[' + str(p) + ']').text
            print(value)
            table_df.append(value)
            table_df_csv = pd.DataFrame(table_df)
        print()

    table_df_csv = pd.DataFrame(table_df_csv.values.reshape(-1, 7),
                    columns=['secWiseDelPosDate', 'SC_CODE', 'Stock', 'Client_Name', 'Series', 'quantityTraded', 'lastPrice'])
    table_df_csv['secWiseDelPosDate'] = pd.to_datetime(table_df_csv['secWiseDelPosDate'], format='%d/%m/%Y')
    driver.close()
    table_df_csv.to_csv(ashish_dir + "\\block_bse.csv", index=None)


if __name__ == '__main__':

    try:
        print('bulk_nse')
        bulk_nse()
    except Exception as e:
        print('err', str(e))
        pass
    try:
        print('block_nse')
        block_nse()
    except Exception as e:
        print('err', str(e))
        pass
    try:
        print('Surveillance_Investigation')
        surveillance_investigation()
    except Exception as e:
        print('err', str(e))
        pass
    try:
        print('fno_combine_OI')
        fno_combine_OI()
    except Exception as e:
        print('err', str(e))
        pass
    try:
        print('eq_band_change')
        eq_band_change()
    except Exception as e:
        print('err', str(e))
        pass
    try:
        print('fii_derivatives')
        fii_derivatives()
    except Exception as e:
        print('err', str(e))
        pass
    try:
        print('participant_wise_OI')
        participant_wise_OI()
    except Exception as e:
        print('err', str(e))
        pass

    print('bulk_bse')
    bulk_bse()
    print('block_bse')
    block_bse()

