import requests
from requests.exceptions import HTTPError
import pandas as pd
import os
import time
import re
from zipfile import ZipFile
from filepath import *
import warnings

warnings.simplefilter(action="ignore", category=Warning)
#github change
#date:- 20210104 (04-01-2021)
#link :-https://links.sgx.com/1.0.0/derivatives-daily/6102/FUTURE.zip


def unzip_folder(bhav_file_path,id):
    try:
        zf = ZipFile(RAW_DIR + '\\' +bhav_file_path)
        zf.extractall(RAW_DIR)
        name=zf.namelist()
        Read_csv(name[0],id)
        zf.close()
    except:
        print("This Date data is Not Avilable")


def Aggrigate(data):
    data = data.assign(COM_DD=1)
    data[['COM_DD','COM_MM', 'COM_YY']]=data[['COM_DD','COM_MM', 'COM_YY']].astype(str)
    data['Expiry_Date'] = data[['COM_DD','COM_MM', 'COM_YY']].agg('-'.join, axis=1)

    data['Expiry_Date'] = pd.to_datetime(data['Expiry_Date'], format='%d-%m-%Y')
    data['Expiry_Date'] = data['Expiry_Date'].dt.strftime('%Y-%m-%d')

    row=data[data['Expiry_Date'].str.contains(min(data['Expiry_Date']))]
    row.rename(columns={'COM':'Symbol'},inplace=True)
    row['VOLUME']=sum(data['VOLUME'])
    row['OINT']=sum(data['OINT'])
    row['Symbol'] = row['Symbol'].str.strip()
    row['Symbol'] = row['Symbol'].replace('IN','SGX_Nifty')
    row.drop(['SERIES','COM_DD'], axis=1,inplace=True)

    data['Expiry_Date'] = pd.to_datetime(data['Expiry_Date'], format='%Y-%m-%d')
    data['Expiry_Date'] = data['Expiry_Date'].dt.strftime('%d-%m-%Y')

    if not os.path.exists(PROCESSED_DIR + '\\' + 'Agg_SGX_Nifty_Future.csv'):
        row.to_csv(PROCESSED_DIR + '\\' + 'Agg_SGX_Nifty_Future.csv', index=False)

    else:
        Historical = pd.read_csv(PROCESSED_DIR + '\\' + 'Agg_SGX_Nifty_Future.csv')
        duplicate = [value for value in row['DATE'].unique().tolist() if value in Historical['DATE'].unique().tolist()]
        if bool(duplicate) == False:
            Historical = pd.concat([row, Historical])
            Historical.sort_values(by=['ID'], inplace=True, ascending=False)

            Historical.to_csv(PROCESSED_DIR + '\\' + 'Agg_SGX_Nifty_Future.csv', index=False)  # ,mode='a'
        else:
            print("This Date Data are Already Exists")

def Read_csv(filename,id):
    try:
        df = pd.read_csv(RAW_DIR + '\\' + str(filename))
        pattern=re.compile(r'\bIN\b')
        filter=df[df['COM'].str.contains(pattern)]
        filter['ID']=id

        if not os.path.exists(PROCESSED_DIR+'\\'+'SGX_Nifty_Future.csv'):
            filter['DATE'] = pd.to_datetime(filter['DATE'], format='%Y%m%d')
            filter['DATE'] = filter['DATE'].dt.strftime('%d-%m-%Y')
            filter.to_csv(PROCESSED_DIR+'\\'+'SGX_Nifty_Future.csv', index=False)

        else:
            Historical = pd.read_csv(PROCESSED_DIR+'\\'+'SGX_Nifty_Future.csv')

            Historical['DATE'] = pd.to_datetime(Historical['DATE'], format='%d-%m-%Y')
            Historical['DATE'] = Historical['DATE'].dt.strftime('%Y-%m-%d')

            filter['DATE'] = pd.to_datetime(filter['DATE'], format='%Y%m%d')
            filter['DATE'] = filter['DATE'].dt.strftime('%Y-%m-%d')

            duplicate = [value for value in filter['DATE'].unique().tolist() if value in Historical['DATE'].unique().tolist()]
            if bool(duplicate) == False:
                Historical = pd.concat([filter, Historical])

                Historical['DATE'] = pd.to_datetime(Historical['DATE'], format='%Y-%m-%d')
                Historical['DATE'] = Historical['DATE'].dt.strftime('%d-%m-%Y')
                Historical.sort_values(by=['ID'], inplace=True, ascending=False)
                Historical.to_csv(PROCESSED_DIR+'\\'+'SGX_Nifty_Future.csv',index=False) #,mode='a'
            else:
                print("This Date Data are Already Exists")
        Aggrigate(filter)

    except Exception as e:
        print("Error",e)

def SGX_Historical(start_id,end_id):
    try:
        while(start_id <= end_id):
            link = 'https://links.sgx.com/1.0.0/derivatives-daily/'+str(start_id)+'/FUTURE.zip'

            time.sleep(10)
            response = requests.get(link, timeout=10)
            response.raise_for_status()

            filename = 'SGX' + str(start_id) + '.zip'
            open(RAW_DIR + '\\' + str(filename), 'wb').write(response.content)
            unzip_folder(filename,start_id)
            start_id=start_id+1
    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')

try:
    historical_data = 0
    if historical_data == 0:
        Historical = pd.read_csv(PROCESSED_DIR + '\\' + 'SGX_Nifty_Future.csv')
        Today_ID=Historical['ID'].max()+1

        start_id = end_id = Today_ID
        SGX_Historical(start_id,end_id)

    else:
        start_id = 6442  # 04-Jan-2021 start id= 6102, 4-jan-2021
        end_id = 6444     #end_id = 6410  # 10-March-2022 ,6409 09-march-2022
        SGX_Historical(start_id,end_id)
except:
    print("This Date data is Not Avilable")

#start_id=6102 # 04-Jan-2021
#end_id=6410 # 10-March-2022 ,6409 09-march-2022
