# This will give you last 10 days delivery flow of your net position
import requests
import xlwings as xw
import pandas as pd
import json
import os
from datetime import datetime
from time import sleep
from datetime import timedelta
import numpy as np
import datetime as dt
from filepath import *

#pd.set_option('display.max_columns', None)

#wb = xw.Book(r'E:\trades\Profit_Loss.xlsx')
wb = xw.Book(r'D:\\Program\\python_ankit\\Q459\\Profit_Loss_Q459.xlsx')
list_excel_input = wb.sheets['Net_Position'].range('B2:C2000').options(index=False).value

print(list_excel_input)
net_position_list =[]
for i in range(0, len(list_excel_input)):
    if list_excel_input[i][1] != '(bl`  ank)' and list_excel_input[i][1] != None and list_excel_input[i][0] != 0.0 :
        print(list_excel_input[i][1])
        if int(list_excel_input[i][1]) > 0:
            stock_dictionary = {'Stock': list_excel_input[i][0].strip(),'Quantity': list_excel_input[i][1] }
            net_position_list.append(stock_dictionary)

net_position_df = pd.DataFrame(net_position_list)

# load historical data
data_folder = os.path.join("D:\Program\python_ankit\ProjectDir\data")
delivery_position_filename_historical = os.path.join(PROCESSED_DIR, "bhav_data_nse_historical.csv")

#  Load list to frames2021
try:
    delivery_position_historical_df = pd.read_csv(delivery_position_filename_historical)
except Exception as error:
    delivery_position_historical_df = pd.DataFrame()
    print("Creating File".format(error))

# Filter stocks which has our position
cond = delivery_position_historical_df['Stock'].isin(net_position_df['Stock'])
delivery_position_historical_df = delivery_position_historical_df[cond]

#pd.set_option('display.max_columns', None)4980rt438ty67r9it,mhio5y49
#pd.set_option('display.max_rows', None)

# Filter stocks for last 10 days data
todaydate = dt.date.today()
last10day = todaydate - timedelta(days=10)
last10day = pd.Timestamp(last10day)
delivery_position_historical_df['secWiseDelPosDate'] = pd.to_datetime(delivery_position_historical_df['secWiseDelPosDate'], format='%d-%m-%Y %H:%M')
delivery_position_historical_df_last10 = delivery_position_historical_df[delivery_position_historical_df['secWiseDelPosDate'] >= last10day]

#delivery_position_historical_df_last10 = delivery_position_historical_df_last10
# Output to Excel
sheet_delivery_flow = wb.sheets("Delivery_flow_Position")
sheet_delivery_flow.range("A1").options().value = delivery_position_historical_df_last10[
        ['Stock', 'secWiseDelPosDate', 'quantityTraded', 'deliveryQuantity', 'deliveryToTradedQuantity','lastPrice','pChange','vwap']].sort_values(by =['Stock','secWiseDelPosDate'])

