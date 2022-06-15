import requests
from requests.exceptions import HTTPError
import pandas as pd
import numpy as np
import os
import time
from datetime import timedelta, date
import datetime as dt
import csv
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.support.select import Select

table_df=[]
krxinrarray=[]

twdinrarray = []
Missed_date = []

# META_DATA_DIR = r'\\192.168.41.190\program\stockdata\metadata'
# PROCESSED_DIR = r'\\192.168.41.190\program\stockdata\processed'

Basepath= r'\\192.168.41.190\program\stockdata\raw'
Filepath=r'\\192.168.41.190\program\stockdata\raw'
driver = webdriver.Chrome(ChromeDriverManager().install())


def TWDCurrencyConverter(filename, mdates):
    url = 'https://exchangerate.guru/twd/inr/1/'
    driver.get(url)
    twd = driver.find_element(By.XPATH, '//input[@data-role="base-input"]')
    inr = driver.find_element(By.XPATH, '//input[@data-role="secondary-input"]')
    try:
        file = open(Basepath + '\\' + str(filename), newline='')
        csv_reader = csv.reader(file)
        first = next(csv_reader)
        list1 = []
        for j in first:
            list1 += j.split(" ")
        Date = pd.to_datetime(list1[0], format='%Y/%m/%d')
        Date = Date.strftime('%d-%m-%Y')
        twdinrarray.append(str(Date))
        rows = [r for r in csv_reader]
        row = rows[4]
        for i in range(1, len(row) - 1):
            twd.clear()
            twd.send_keys(str(row[i]))
            time.sleep(3)
            inrval = inr.get_attribute("value")
            inrval = inrval.replace(",", "")
            inrval = round((float(inrval) / 10000000), 0)
            twdinrarray.append(inrval)

    except:
        Missed_date.append(mdates)
        print("File Not Found", Missed_date)


def TWDCollectionOfDates():
    try:
        All_date = []
        def daterange(date1, date2):
            for n in range(int((date2 - date1).days) + 1):
                yield date1 + timedelta(n)
        weekdays = [5, 6]
        for dt1 in daterange(start_dt, end_dt):
            if dt1.weekday() not in weekdays:  # to print only the weekdates
                a = dt1.strftime("%Y%m%d")
                All_date.append(a)
        # TWD HOLIDAYS FILE
        TWDHolidays = pd.read_csv(Filepath + '\\' + 'TWDHolidays.csv')

        TWDHolidays['Date'] = pd.to_datetime(TWDHolidays['Date'], format='%d-%m-%Y')
        TWDHolidays['Date'] = TWDHolidays['Date'].dt.strftime('%Y-%m-%d')

        for h in TWDHolidays['Date']:
            h = h.replace("-", "")
            if str(h) in All_date:
                All_date.remove(str(h))  # Remove All Holiday Dates From Main Data
        return All_date
    except:
        print("There is No Data Available for this Dates")


def TWD():
    try:
        All_date = TWDCollectionOfDates()
        for date1 in All_date:
            link = 'https://www.twse.com.tw/en/fund/BFI82U?response=csv&dayDate=' + str(date1) + '&weekDate=' + str(date1) + '&monthDate=' + str(date1) + '&type=day'
            try:
                time.sleep(50)
                response = requests.get(link, timeout=10)
                response.raise_for_status()

                filename = 'BFI82U_day_' + str(date1) + '.csv'
                open(Basepath + '\\' + str(filename), 'wb').write(response.content)
                TWDCurrencyConverter(filename, str(date1))

            except HTTPError as http_err:
                print(f'HTTP error occurred: {http_err}')  # Python 3.6
            except Exception as err:
                print(f'Other error occurred: {err}')

        table_df_csv = pd.DataFrame(twdinrarray)
        table_df_csv = pd.DataFrame(table_df_csv.values.reshape(-1, 4),
                                    columns=['Date', 'TWD_Buy', 'TWD_Sell', 'TWD_Net_Buy'])
        return table_df_csv
    except:
        print("Something Went Wrong with TWD Data")


def KRXCurrencyConverter(krwdata):
    try:
        url='https://exchangerate.guru/krw/inr/1/'
        driver.get(url)
        krw=driver.find_element(By.XPATH,'//input[@data-role="base-input"]')
        inr=driver.find_element(By.XPATH,'//input[@data-role="secondary-input"]')
        for krwd in krwdata:
            krw.clear()
            krw.send_keys(str(krwd))
            time.sleep(3)
            inrval = inr.get_attribute("value")
            inrval=inrval.replace(",","")
            inrval=round((float(inrval)/10),0)
            krxinrarray.append(inrval)
    except:
        print("KRX Currency Data is Not Converted")


def KRXCollectionOfDates():
    try:
        All_date = []
        def daterange(date1, date2):
            for n in range(int((date2 - date1).days) + 1):
                yield date1 + timedelta(n)
        weekdays = [5, 6]
        for dt1 in daterange(start_dt, end_dt):
            if dt1.weekday() not in weekdays:  # to print only the weekdates
                a = dt1.strftime("%Y%m%d")
                All_date.append(a)
        Holidays = pd.read_excel(Filepath + '\\' + 'KRXHolidays.xlsx')
        Holidays['Date'] = pd.to_datetime(Holidays['Date'], format='%d-%m-%Y')
        Holidays['Date'] = Holidays['Date'].dt.strftime('%Y-%m-%d')

        for h in Holidays['Date']:
            h = h.replace("-", "")
            if str(h) in All_date:
                All_date.remove(str(h))

        return All_date
    except:
        print("Something Went Wrong With Dates")


# Scrape Data From KRX Links
def ScrapeKRX(date):
    try:
        # Link 1
        time.sleep(5)
        driver.get('http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020301')
        driver.minimize_window()

        time.sleep(3)
        Startdatebox1 = driver.find_element(By.XPATH,'//input[@id="strtDd"]')
        Enddatebox1 = driver.find_element(By.XPATH,'//input[@id="endDd"]')

        Startdatebox1.clear()
        Enddatebox1.clear()

        time.sleep(3)
        Startdatebox1.send_keys(str(date))
        Enddatebox1.send_keys(str(date))

        day1=driver.find_element(By.XPATH,'//button[@class="cal-btn-range1d"]')
        day1.click()

        time.sleep(3)
        searchbutton=driver.find_element(By.XPATH,'//a[@id="jsSearchButton"]')
        searchbutton.click()
        time.sleep(4)

        rows=len(driver.find_elements(By.XPATH,'//table[@class="CI-GRID-BODY-TABLE"][1]/tbody/tr[11]/td'))
        for r in range(5, rows+1):
            value = driver.find_element(By.XPATH,'//table[@class="CI-GRID-BODY-TABLE"][1]/tbody/tr[11]/td[' + str(r) + ']').text
            table_df.append(value)

        # Link 2
        driver.get('http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201050302')
        time.sleep(5)

        Startdatebox = driver.find_element(By.XPATH, '//input[@id="strtDdBox1"]')
        Enddatebox = driver.find_element(By.XPATH, '//input[@id="endDdBox1"]')
        Startdatebox.clear()
        Enddatebox.clear()
        time.sleep(2)
        Startdatebox.send_keys(str(date))
        Enddatebox.send_keys(str(date))
        time.sleep(2)


        link2day1 = driver.find_element(By.XPATH,'//td[@class="dateBox1"]/div/div/button[2]')
        link2day1.click()
        time.sleep(3)

        dropdown = driver.find_element(By.XPATH,'//select[@name="isuCd"]')
        dropdownmenu = Select(dropdown)
        dropdownmenu.select_by_value("KRDRVFUTTT")            #Future Total("KRDRVFUTTT")
        time.sleep(3)

        searchbutton = driver.find_element(By.XPATH,'//a[@id="jsSearchButton"]')
        searchbutton.click()
        time.sleep(4)
        rows = len(driver.find_elements(By.XPATH,'//table[@class="CI-GRID-BODY-TABLE"][1]/tbody/tr[10]/td'))
        for r in range(5, rows+1):
            value = driver.find_element(By.XPATH,'//table[@class="CI-GRID-BODY-TABLE"][1]/tbody/tr[10]/td[' + str(r) + ']').text
            table_df.append(value)
        time.sleep(2)
        dropdownmenu.select_by_value("KRDRVOPTTT")  # Option Total("KRDRVFUTTT")
        searchbutton.click()
        time.sleep(4)
        for r in range(5, rows + 1):
            value = driver.find_element(By.XPATH,'//table[@class="CI-GRID-BODY-TABLE"][1]/tbody/tr[10]/td[' + str(r) + ']').text
            table_df.append(value)

    except ConnectionError as e:
        print("Connection Error ",e)


def KRX():
    All_date = KRXCollectionOfDates()
    for date1 in All_date:
        Date = pd.to_datetime(date1, format='%Y%m%d')
        Date = Date.strftime('%d-%m-%Y')
        krxinrarray.append(str(Date))
        table_df.clear()
        ScrapeKRX(date1)
        KRXCurrencyConverter(table_df)

    table_df_csv = pd.DataFrame(krxinrarray)
    table_df_csv = pd.DataFrame(table_df_csv.values.reshape(-1, 10),
                                columns=['Date', 'KRX_Cash_Sell', 'KRX_Cash_Buying', 'KRX_Cash_net_buy',
                                         'KRX_Future_Sell', 'KRX_Future_Buying', 'KRX_Future_net_buy',
                                         'KRX_Option_Sell', 'KRX_Option_Buying', 'KRX_Option_net_buy'])
    return table_df_csv

try:
    historical_data = 0
    if historical_data == 0:
        start_dt = end_dt = date.today()
        twdtable = TWD()
        krxtable = KRX()

        df_merge = pd.merge(twdtable, krxtable, on=['Date'], how='outer')
        if not os.path.exists(Filepath + '\\' + 'HistoricalTWDKRX.xlsx'):
            df_merge.to_excel(Filepath + '\\' + 'HistoricalTWDKRX.xlsx', index=False)
        else:
            Historical = pd.read_excel(Filepath + '\\' + 'HistoricalTWDKRX.xlsx')
            Historical['Date'] = pd.to_datetime(Historical['Date'], format='%d-%m-%Y')
            Historical['Date'] = Historical['Date'].dt.strftime('%Y-%m-%d')
            df_merge['Date'] = pd.to_datetime(df_merge['Date'], format='%d-%m-%Y')
            df_merge['Date'] = df_merge['Date'].dt.strftime('%Y-%m-%d')

            if df_merge['Date'].max() != Historical['Date'].max():
                Historical = pd.concat([df_merge, Historical])
                Historical['Date'] = pd.to_datetime(Historical['Date'], format='%Y-%m-%d')
                Historical['Date'] = Historical['Date'].dt.strftime('%d-%m-%Y')
                Historical.to_excel(Filepath + '\\' + 'HistoricalTWDKRX.xlsx', index=False)
            else:
                print("This Date Data are Already Exists")

    else:
        start_dt = date(2021,12,24)     # Starting Date in %y,%m,%d Format Not use Zero Padding
        end_dt = date(2021,12,24)        # Ending Date in %y,%m,%d Format Not use Zero Padding

        twdtable=TWD()
        krxtable=KRX()
        df_merge = pd.merge(twdtable, krxtable, on=['Date'], how='outer')

        if not os.path.exists(Filepath + '\\' + 'HistoricalTWDKRX.xlsx'):
            df_merge.to_excel(Filepath + '\\' + 'HistoricalTWDKRX.xlsx', index=False)
        else:
            Historical = pd.read_excel(Filepath + '\\' + 'HistoricalTWDKRX.xlsx')

            Historical['Date'] = pd.to_datetime(Historical['Date'], format='%d-%m-%Y')
            Historical['Date'] = Historical['Date'].dt.strftime('%Y-%m-%d')
            df_merge['Date'] = pd.to_datetime(df_merge['Date'], format='%d-%m-%Y')
            df_merge['Date'] = df_merge['Date'].dt.strftime('%Y-%m-%d')

            if df_merge['Date'].max() != Historical['Date'].max():
                Historical = pd.concat([df_merge, Historical])
                Historical = Historical.sort_values(by='Date', ascending=False)
                Historical['Date'] = pd.to_datetime(Historical['Date'], format='%Y-%m-%d')
                Historical['Date'] = Historical['Date'].dt.strftime('%d-%m-%Y')
                Historical.to_excel(Filepath + '\\' + 'HistoricalTWDKRX.xlsx', index=False)
            else:
                print("This Date Data are Already Exists")
except:
    print("There is No Data Available for Todays date")

driver.close()