import pandas as pd
import numpy as np
import datetime as dt
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import os
from utils import *

RAW_DIR = r'\\192.168.41.190\program\stockdata\raw'
cd = dt.datetime.today().strftime("%d-%m-%Y")
PROCESSED_DIR = r'\\192.168.41.190\program\stockdata\processed'
METADATA_DIR = r"\\192.168.41.190\program\stockdata\metadata"


def bse_nse_merge():
    bse_df = pd.read_excel(RAW_DIR + '\\BSE ATH & 52 WK HIGH.xlsx')
    nse_df = pd.read_excel(RAW_DIR + '\\NSE ATH & 52 WK HIGH.xlsx')
    nse_df = nse_df[['CD_NSE Symbol', 'SC_NSE 52 Wk High Date', 'SC_NSE 52 Wk High Price',
                     'SC_NSE All Time High Date', 'SC_NSE All Time High Price']]
    nse_df.rename(
        columns={'CD_NSE Symbol': 'Stock', 'SC_NSE 52 Wk High Date': '52 Wk High Date',
                 'SC_NSE 52 Wk High Price': '52 Wk High Price', 'SC_NSE All Time High Date': 'all_time_high_Date',
                 'SC_NSE All Time High Price': 'all_time_high_price'}, inplace=True)
    bse_df = bse_df[['CD_BSE Code', 'SC_BSE 52 Wk High Date', 'SC_BSE 52 Wk High Price',
                     'SC_BSE All Time High Date', 'SC_BSE All Time High Price']]
    bse_df.rename(
        columns={'CD_BSE Code': 'Security Code', 'SC_BSE 52 Wk High Date': '52 Wk High Date',
                 'SC_BSE 52 Wk High Price': '52 Wk High Price', 'SC_BSE All Time High Date': 'all_time_high_Date',
                 'SC_BSE All Time High Price': 'all_time_high_price'}, inplace=True)
    prev_yr_df = pd.concat([bse_df, nse_df], ignore_index=True, sort=False)
    prev_yr_df['52 Wk High Date'] = pd.to_datetime(prev_yr_df['52 Wk High Date'], format='%d-%m-%Y')
    prev_yr_df['all_time_high_Date'] = pd.to_datetime(prev_yr_df['all_time_high_Date'], format='%d-%m-%Y')
    prev_yr_df = prev_yr_df[prev_yr_df['all_time_high_Date'].notna()]
    prev_yr_df = prev_yr_df[prev_yr_df['52 Wk High Date'].notna()]
    prev_yr_df['52 Wk High Date'] = prev_yr_df['52 Wk High Date'].dt.strftime('%d-%m-%Y')
    prev_yr_df['all_time_high_Date'] = prev_yr_df['all_time_high_Date'].dt.strftime('%d-%m-%Y')
    prev_yr_df.to_csv(METADATA_DIR + '\\alltime52wkhigh.csv', index=None)


def scrape_bse():
    driver = webdriver.Chrome(ChromeDriverManager().install())
    driver.get("https://www.bseindia.com/markets/equity/EQReports/HighLow.html?Flag=H")
    driver.minimize_window()
    pd.set_option('display.max_columns', None)

    def scrape_table():
        rows = len(driver.find_elements_by_xpath(
            '//*[@id="fontSize"]/div[2]/div[1]/div[1]/div/div/div/div[2]/table/tbody/tr/td/table[1]/tbody/tr'))
        cols = len(driver.find_elements_by_xpath(
            '//*[@id="fontSize"]/div[2]/div[1]/div[1]/div/div/div/div[2]/table/tbody/tr/td/table[1]/tbody/tr[1]/td'))
        print(rows)  # ic(rows)
        print(cols)  # ic(cols)

        table_df = []
        # Printing the data of the table
        for r in range(1, rows + 1):
            for p in range(1, cols + 1):
                # obtaining the text from each column of the table
                value = driver.find_element_by_xpath(
                    '//*[@id="fontSize"]/div[2]/div[1]/div[1]/div/div/div/div[2]/table/tbody/tr/td/table[1]/tbody/tr[' + str(
                        r) + ']/td[' + str(p) + ']').text

                print(value)
                table_df.append(value)
                table_df_csv = pd.DataFrame(table_df)
                print()

        table_df_csv = pd.DataFrame(table_df_csv.values.reshape(-1, 6),
                                    columns=['Security Code', 'Security Name', 'LTP', '52 Weeks High',
                                             'Previous 52 Weeks High(Price/Date)', 'All Time High(Price/Date)'])
        table_df_csv[['Previous 52 Weeks High Price', 'Previous 52 Weeks High Date']] = table_df_csv[
            'Previous 52 Weeks High(Price/Date)'].str.split("(", expand=True)
        table_df_csv[['All Time High Price', 'All Time High Date']] = table_df_csv[
            'All Time High(Price/Date)'].str.split("(", expand=True)

        table_df_csv['All Time High Date'] = table_df_csv['All Time High Date'].str.replace('[(,)]', '')
        table_df_csv['Previous 52 Weeks High Date'] = table_df_csv['Previous 52 Weeks High Date'].str.replace('[(,)]',
                                                                                                              '')
        table_df_csv = table_df_csv[table_df_csv['All Time High Date'] != '-']
        table_df_csv = table_df_csv[table_df_csv['Previous 52 Weeks High Date'] != '-']
        table_df_csv['All Time High Date'] = pd.to_datetime(table_df_csv['All Time High Date'], format='%d %b %Y')
        table_df_csv['All Time High Date'] = table_df_csv['All Time High Date'].dt.strftime('%d-%m-%Y')

        table_df_csv['Previous 52 Weeks High Date'] = pd.to_datetime(table_df_csv['Previous 52 Weeks High Date'],
                                                                     format='%d %b %Y')
        table_df_csv['Previous 52 Weeks High Date'] = table_df_csv['Previous 52 Weeks High Date'].dt.strftime(
            '%d-%m-%Y')

        table_df_csv.drop('Previous 52 Weeks High(Price/Date)', axis=1, inplace=True)
        table_df_csv.drop('All Time High(Price/Date)', axis=1, inplace=True)

        return table_df_csv

    try:
        table_df_csv = scrape_table()
        table_df_csv.to_csv(RAW_DIR + '\\' + 'BSE_Data_' + str(cd) + '.csv', index=None)
        i = 0
        pagenumber = 1
        while i < 12:
            i = i + 1
            try:
                page_link = driver.find_element(By.XPATH, '//li[@class="pagination-page ng-scope"][{}]/a'.format(i))
                page_number = driver.find_element(By.XPATH, '//li[@class="pagination-page ng-scope"][{}]'.format(i))
                print("page_number:", page_number.text)
                print("page_var", pagenumber)
                if page_number.text == "...":
                    i = 1
                elif int(page_number.text) < pagenumber:
                    print("page_number:", page_number.text)
                    continue
                page_link.click()
                pagenumber = pagenumber + 1
                table_df_csv1 = scrape_table()
                table_df_csv1.to_csv(RAW_DIR + '\\' + 'BSE_Data_' + str(cd) + '.csv', mode='a', index=None,
                                     header=None)
            except:
                break
    except Exception as e:
        print(e)
    driver.close()

# im test
def all_time_high_update():
    alltime52wkhigh = pd.read_csv(METADATA_DIR + '\\alltime52wkhigh.csv')

    alltime52wkhigh['52 Wk High Date'] = pd.to_datetime(alltime52wkhigh['52 Wk High Date'], format='%d-%m-%Y')
    alltime52wkhigh['all_time_high_Date'] = pd.to_datetime(alltime52wkhigh['all_time_high_Date'], format='%d-%m-%Y')
    bhav_data_nse = pd.read_csv(PROCESSED_DIR + '\\bhav_data_nse_historical.csv')

    # updating NSE File
    bhav_data_nse['secWiseDelPosDate'] = pd.to_datetime(bhav_data_nse['secWiseDelPosDate'], format='%d-%m-%Y %H:%M')
    # max_date = bhav_data_nse['secWiseDelPosDate'].max()
    max_date = np.datetime64(bhav_data_nse['secWiseDelPosDate'].max())

    max_date_df = bhav_data_nse[bhav_data_nse['secWiseDelPosDate'] == max_date]
    temp_df1 = max_date_df[['Stock', 'HighPrice', 'secWiseDelPosDate']]

    df_merge1 = pd.merge(temp_df1, alltime52wkhigh, on=['Stock'], how='right')

    df_merge1['all_time_high_Date'] = pd.to_datetime(df_merge1['all_time_high_Date'], format='%d-%m-%Y')
    df_merge1['52 Wk High Date'] = pd.to_datetime(df_merge1['52 Wk High Date'], format='%d-%m-%Y')

    df_merge1['52 Wk High Date'] = np.where(df_merge1['HighPrice'] > df_merge1['52 Wk High Price'], max_date,
                                            df_merge1['52 Wk High Date'])
    df_merge1['all_time_high_Date'] = np.where(df_merge1['HighPrice'] > df_merge1['all_time_high_price'], max_date,
                                               df_merge1['all_time_high_Date'])
    df_merge1["all_time_high_price"] = df_merge1[["all_time_high_price", "HighPrice"]].max(axis=1)
    df_merge1["52 Wk High Price"] = df_merge1[["52 Wk High Price", "HighPrice"]].max(axis=1)

    max_date = np.datetime64(dt.datetime.today())

    df_merge1['all_time_high_Date'] = pd.to_datetime(df_merge1['all_time_high_Date'], format='%Y-%m-%d')
    df_merge1['52 Wk High Date'] = pd.to_datetime(df_merge1['52 Wk High Date'], format='%Y-%m-%d')
    df_merge1['date_diff_frm_today_ath'] = (max_date - df_merge1['all_time_high_Date']).dt.days
    df_merge1['date_diff_frm_today_52_week'] = (max_date - df_merge1['52 Wk High Date']).dt.days
    df_merge1 = df_merge1[["Security Code", "Stock", "52 Wk High Date", "52 Wk High Price",
                           "all_time_high_Date", "all_time_high_price", "date_diff_frm_today_ath",
                           "date_diff_frm_today_52_week"]]

    #  updating BSE DATA
    bse_today_df = pd.read_csv(RAW_DIR + '\\' + 'BSE_Data_' + str(cd) + '.csv')
    bse_today_df['All Time High Date'] = pd.to_datetime(bse_today_df['All Time High Date'], format='%d-%m-%Y')
    bse_today_df['Previous 52 Weeks High Date'] = pd.to_datetime(bse_today_df['Previous 52 Weeks High Date'],
                                                                 format='%d-%m-%Y')
    bse_today_df['All Time High Price'] = bse_today_df['All Time High Price'].str.replace(',', '').astype(float)
    bse_today_df['All Time High Price'].apply(pd.to_numeric)
    bse_today_df['52 Weeks High'] = bse_today_df['52 Weeks High'].astype(str)
    bse_today_df['52 Weeks High'] = bse_today_df['52 Weeks High'].str.replace(',', '').astype(float)
    bse_today_df['52 Weeks High'].apply(pd.to_numeric)

    temp_df = bse_today_df[['Security Code', 'Security Name', 'All Time High Price', 'All Time High Date',
                            '52 Weeks High', 'Previous 52 Weeks High Date']]

    df_merge = pd.merge(temp_df, df_merge1, on=['Security Code'], how='right')
    df_merge['52 Wk High Date'] = np.where(df_merge['52 Weeks High'] > df_merge['52 Wk High Price'],
                                           df_merge['Previous 52 Weeks High Date'], df_merge['52 Wk High Date'])
    df_merge['all_time_high_Date'] = np.where(df_merge['All Time High Price'] > df_merge['all_time_high_price'],
                                              df_merge['All Time High Date'], df_merge['all_time_high_Date'])
    df_merge["52 Wk High Price"] = df_merge[["52 Weeks High", "52 Wk High Price"]].max(axis=1)
    df_merge["all_time_high_price"] = df_merge[["All Time High Price", "all_time_high_price"]].max(axis=1)
    df_merge1['date_diff_frm_today_ath'] = (max_date - df_merge1['all_time_high_Date']).dt.days
    df_merge1['date_diff_frm_today_ath'] = (max_date - df_merge1['all_time_high_Date']).dt.days

    df_merge = df_merge[["Security Code", "Stock", "52 Wk High Date", "52 Wk High Price",
                         "all_time_high_Date", "all_time_high_price", "date_diff_frm_today_ath",
                         "date_diff_frm_today_52_week"]]
    df_merge['52 Wk High Date'] = df_merge['52 Wk High Date'].dt.strftime('%d-%m-%Y')
    df_merge['all_time_high_Date'] = df_merge['all_time_high_Date'].dt.strftime('%d-%m-%Y')
    df_merge.to_csv(METADATA_DIR + '\\alltime52wkhigh.csv', index=None)


if __name__ == '__main__':
    file = os.path.basename(__file__)
    try:
        flag = 0
        if flag == 1:
            bse_nse_merge()
        scrape_bse()
        all_time_high_update()
        execution_status('pass', file, 'success', cd, 1)
    except Exception as e:
        print(e)
        sendmail_err(file, e)
        execution_status(str(e), file, 'fail', cd, 0)
