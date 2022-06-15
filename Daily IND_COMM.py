import pandas as pd
from filepath import *
import xlwings as xw

df = pd.read_csv(PROCESSED_DIR + '\\' + 'Historical_Commodities_Data.csv')
df['Date'] = pd.to_datetime(df['Date'], format='%d-%m-%Y')
df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
maxdate=df['Date'].max()
maxdate = pd.to_datetime(maxdate, format='%Y-%m-%d')
maxdate = maxdate.strftime('%d-%m-%Y')
df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
df['Date'] = df['Date'].dt.strftime('%d-%m-%Y')

#2022-03-14
#'14-03-2022'
maxrow=df[df['Date'] == str(maxdate)]

df1=maxrow[(maxrow['Strength'] == 1) & (maxrow['Strength_ratio'] > 1)]
df2=maxrow[(maxrow['Engulfing'] == 1) & (maxrow['Strength_ratio'] > 1)]
inddf=pd.read_csv(PROCESSED_DIR + '\\' + 'Ind_In_Close_Historical.csv')
inddf['Date'] = pd.to_datetime(inddf['Date'], format='%d-%m-%Y')
inddf['Date'] = inddf['Date'].dt.strftime('%Y-%m-%d')
indmaxdate= inddf['Date'].max()
indmaxdate = pd.to_datetime(indmaxdate, format='%Y-%m-%d')
indmaxdate = indmaxdate.strftime('%d-%m-%Y')
inddf['Date'] = pd.to_datetime(inddf['Date'], format='%Y-%m-%d')
inddf['Date'] = inddf['Date'].dt.strftime('%d-%m-%Y')

#'14-03-2022'
indmaxrow=inddf[inddf['Date'] == str(indmaxdate)]
inddf1=indmaxrow[(indmaxrow['Strength'] == 1) & (indmaxrow['Strength_ratio'] > 1)]
inddf2=indmaxrow[(indmaxrow['Engulfing'] == 1)]
concat=pd.concat([inddf1,inddf2,df1,df2],axis=0,ignore_index=True)


workbook = xw.Book(REPORT_DIR + '\\' + 'fut_OI_historical.xlsm')
ws1 = workbook.sheets['indx_comm']
ws1.clear()
ws1.range('A1').options(index=False).value = concat
workbook.save()

#concat.to_csv(RAW_DIR + '\\' + 'daily_ind_comm.csv', index=None)

