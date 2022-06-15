import pandas as pd
import os
import time
from datetime import timedelta,date
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium import webdriver
from filepath import *
from Historical_Daily_Snapshot import Analysis
driver = webdriver.Chrome(ChromeDriverManager().install())

def Commodities_Data(url,start_dt,end_dt):
    time.sleep(5)
    driver.get(str(url))
    time.sleep(10)
    Datepicker = driver.find_element(By.XPATH, '//div[@id="flatDatePickerCanvasHol"]/div[2]/span')
    Datepicker.click()

    Start_Date=driver.find_element(By.XPATH,'//input[@id="startDate"]')
    End_date=driver.find_element(By.XPATH,'//input[@id="endDate"]')

    Start_Date.clear()
    End_date.clear()
    time.sleep(2)
    Start_Date.send_keys(str(start_dt))
    End_date.send_keys(str(end_dt))

    time.sleep(2)

    ApplyButton = driver.find_element(By.XPATH, '//a[@id="applyBtn"]')
    ApplyButton.click()
    time.sleep(5)

    Commodities = driver.find_element(By.XPATH, '//div[@class="instrumentFloaterInner"]/div[1]/span[1]').text
    time.sleep(5)
    rows = len(driver.find_elements(By.XPATH,'//div[@id="results_box"]/table[1]/tbody/tr'))
    cols = len(driver.find_elements(By.XPATH,'//div[@id="results_box"]/table[1]/tbody/tr[1]/td'))
    table_df = []

    for r in range(1, rows + 1):
        for p in range(1, cols + 1):
            value = driver.find_element(By.XPATH,'//div[@id="results_box"]/table/tbody/tr[' + str(r) + ']/td[' + str(p) + ']').text
            print(value)
            table_df.append(value)
        table_df.append(str(Commodities))

    table_df_csv = pd.DataFrame(table_df)
    table_df_csv = pd.DataFrame(table_df_csv.values.reshape(-1, 8),columns=['Date', 'Close', 'Open', 'High','Low', 'Volume','Change_perct','Index_Name'])

    if (Commodities == 'Gold' or Commodities == 'Aluminium'):
        table_df_csv['Open'] = table_df_csv['Open'].str.replace(',', '')
        table_df_csv['Close'] = table_df_csv['Close'].str.replace(',', '')
        table_df_csv['High'] = table_df_csv['High'].str.replace(',', '')
        table_df_csv['Low'] = table_df_csv['Low'].str.replace(',', '')

    #Mar 10, 2022
    if not os.path.exists(PROCESSED_DIR + '\\' + 'Historical_Commodities_Data.csv'):
        table_df_csv['Date'] = pd.to_datetime(table_df_csv['Date'], format='%b %d, %Y')
        table_df_csv['Date'] = table_df_csv['Date'].dt.strftime('%Y-%m-%d')
        table_df_csv.sort_values(by=['Date'], inplace=True, ascending=True)
        table_df_csv['Date'] = pd.to_datetime(table_df_csv['Date'], format='%Y-%m-%d')
        table_df_csv['Date'] = table_df_csv['Date'].dt.strftime('%d-%m-%Y')
        Analysis(table_df_csv, 'Historical_Commodities_Data.csv')

    else:
        Historical = pd.read_csv(PROCESSED_DIR + '\\' + 'Historical_Commodities_Data.csv')

        Historical['Date'] = pd.to_datetime(Historical['Date'], format='%d-%m-%Y')
        Historical['Date'] = Historical['Date'].dt.strftime('%Y-%m-%d')
        table_df_csv['Date'] = pd.to_datetime(table_df_csv['Date'], format='%b %d, %Y')
        table_df_csv['Date'] = table_df_csv['Date'].dt.strftime('%Y-%m-%d')

        duplicate = [value for value in list(zip(table_df_csv['Date'],table_df_csv['Index_Name'])) if value in list(zip(Historical['Date'],Historical['Index_Name']))]
        if bool(duplicate) == False:
            table_df_csv['Change_perct'] = table_df_csv['Change_perct'].str.replace('%', '')
            table_df_csv['Volume'] = table_df_csv['Volume'].str.replace('K', '')
            table_df_csv['Volume']= pd.to_numeric(table_df_csv['Volume'], errors='coerce') *1000

            Historical = pd.concat([table_df_csv, Historical])
            Historical.sort_values(by=['Date'], inplace=True, ascending=True)
            Historical['Date'] = pd.to_datetime(Historical['Date'], format='%Y-%m-%d')
            Historical['Date'] = Historical['Date'].dt.strftime('%d-%m-%Y')

            Analysis(Historical, 'Historical_Commodities_Data.csv')
        else:
            print("This Date Data are Already Exists")
    print(table_df_csv)

try:
    Links=['https://www.investing.com/commodities/copper-historical-data',
           'https://www.investing.com/commodities/aluminum-historical-data',
           'https://www.investing.com/commodities/gold-historical-data',
           'https://www.investing.com/commodities/silver-historical-data',
           'https://www.investing.com/commodities/crude-oil-historical-data',
           'https://www.investing.com/commodities/natural-gas-historical-data',
           'https://www.investing.com/commodities/steel-scrap-historical-data']

    historical_data = 1
    if historical_data == 0:
        start_dt = end_dt = date.today().strftime('%m/%d/%Y')
        for url in Links:
            Commodities_Data(url,start_dt,end_dt)
    else:
        start_dt = date(2022,3,29).strftime('%m/%d/%Y')     # Starting Date in %d,%m,%Y Format Not use Zero Padding
        end_dt = date(2022,3,29).strftime('%m/%d/%Y')        # Ending Date in %d,%-m,%Y Format Not use Zero Padding

        for url in Links:
            Commodities_Data(url,start_dt,end_dt)
except:
    print("This Date data is Not Avilable or Ads Appears")

time.sleep(10)
driver.close()
