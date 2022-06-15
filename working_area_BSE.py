import xlwings as xw
from utils import *
import numpy as np
from datetime import datetime as datetime_datetime
timenow = datetime_datetime.now().strftime('%H:%M')

date_today = datetime.date.today() #- datetime.timedelta(days=1)
date_today, prev_date, fut_date = get_market_prev_date_fut_date(date_today)

date_today = date_today.strftime('%d%m%y')

# reading files and merging data
delivery_position_df = pd.read_csv(PROCESSED_DIR + '\\del_data_BSE' + '\\bse_delivery_data_' + date_today + '.csv')
delivery_position_df = delivery_position_df[delivery_position_df['pChange'] != '-']
delivery_position_df['secWiseDelPosDate'] = pd.to_datetime(delivery_position_df['secWiseDelPosDate'],
                                                           format='%d-%m-%Y %H:%M')
metadata_df = pd.read_csv(METADATA_DIR + '\\StockMetadata.csv',
                          usecols=['Symbol', 'companyName', 'MarketCap', 'issued_shares', 'Sector', 'token', 'Industry',
                                   'script_code'])
average_df = pd.read_csv(METADATA_DIR + '\\Average_bse.csv')[['SCRIP CODE', 'DEL_norm_mean']]
average_df = average_df.rename(columns={"SCRIP CODE": "script_code"})


merged = metadata_df.merge(average_df, how='right', left_on='script_code', right_on='script_code')
final_merged = delivery_position_df.merge(merged, how='left', left_on='SC_CODE', right_on='script_code')
final_merged.drop(['script_code'], inplace=True, axis=1)
final_merged['vwap'] = final_merged['vwap'].str.replace(',', '').astype(float)

#  norm_mean is Delivery norm mean
final_merged['Times_of_norm_mean'] = final_merged['deliveryQuantity'].div(final_merged['DEL_norm_mean']) \
    .replace(np.inf, 0).round(1)

final_merged['Greater_than_Avg'] = np.where((final_merged['deliveryQuantity'] > final_merged['DEL_norm_mean']), True
                                            , False)
final_merged['Price_Trend'] = np.where(final_merged['pChange'] >= 0,
                                       (np.where(final_merged['lastPrice'] >= final_merged['vwap'], 'PosAboveATP',
                                                 'Positive')),
                                       (np.where(final_merged['lastPrice'] <= final_merged['vwap'], 'NegBelowATP',
                                                 'Negative')))

final_merged['Morning'] = np.where(final_merged['MarketCap'] == 'LARGE', 1.5,
                                   (np.where(final_merged['MarketCap'] == 'Mid', 2,
                                             (np.where(((
                                                                final_merged['MarketCap'] == 'Small') | (
                                                                final_merged['MarketCap'] == 'VSM')),
                                                       3, 1)))))
final_merged['Evening'] = np.where(final_merged['MarketCap'] == 'LARGE', 2,
                                   (np.where(final_merged['MarketCap'] == 'Mid', 3, (
                                       np.where(((final_merged['MarketCap'] == 'Small') | (
                                                   final_merged['MarketCap'] == 'VSM')), 5, 2)))))
final_merged['Above_20D_avg'] = 0  # np.where((final_merged['20DMA_price']<final_merged['lastPrice']),True,False)
final_merged['combined1'] = (final_merged['deliveryQuantity'] * final_merged['lastPrice'])
final_merged['combined'] = final_merged['combined1'].round(1)
final_merged['Abv_30L'] = np.where(final_merged['combined'] > 1000000, 1, 0)

final_merged['time'] = final_merged['secWiseDelPosDate'].dt.time

final_merged1 = final_merged.dropna(subset=['time'])

final_merged1['is_evening'] = np.where(final_merged1['time'] < pd.to_datetime('12:00:00').time(), 0, 1)

final_merged1['Star'] = np.where(final_merged1['is_evening'] == 0, (
    np.where(((final_merged1['Times_of_norm_mean']) > (final_merged1['Morning'])), 'Morning Star', '')), (
                                     np.where(((final_merged1['Times_of_norm_mean']) > (final_merged1['Evening'])),
                                              'EOD Star', '')))
final_merged1 = final_merged1.drop(['combined1', 'combined', 'time'], axis=1)

final_merged1['20DMA_price'] = 0
final_merged1['norm_mean'] = final_merged1['DEL_norm_mean']
final_merged1 = final_merged1[
    ['Stock', 'secWiseDelPosDate', 'deliveryQuantity', 'quantityTraded', 'deliveryToTradedQuantity', 'lastPrice',
     'pChange', 'vwap', 'companyName', 'MarketCap', 'issued_shares',
     '20DMA_price', 'DEL_norm_mean', 'Times_of_norm_mean', 'Price_Trend',
     'Morning', 'Evening', 'Above_20D_avg', 'Abv_30L', 'Star', 'Sector', 'Industry', 'token', 'SC_CODE', 'Series']]

final_merged1['issued_shares'] = final_merged1['issued_shares'].replace("-", "-1")
final_merged1['del_perct_issued_shares'] = round(
    (final_merged1['deliveryQuantity'] / final_merged1['issued_shares'].astype(float)) * 100, 1)
final_merged1['del_perct_isud_gt_1'] = np.where(final_merged1['del_perct_issued_shares'] >= 0.5, 0.5, 0)
final_merged1['del_perct_isud_gt_1'] = np.where(final_merged1['del_perct_issued_shares'] >= 1, 1,
                                                final_merged1['del_perct_isud_gt_1'])
final_merged1['del_perct_isud_gt_1'] = np.where(final_merged1['del_perct_issued_shares'] >= 2, 2,
                                                final_merged1['del_perct_isud_gt_1'])

# merge calender_results
calendar_results = pd.read_csv(METADATA_DIR + '\\results_calendar.csv')[['Security Name', 'Result Date']]
calendar_results = calendar_results.rename(columns={"Security Name": "Stock"})
final_merged1 = pd.merge(final_merged1, calendar_results, on=['Stock'], how='left')


data_excel_file = REPORT_DIR + "\\Equity_Live_Data_bse.xlsm"
wb = xw.Book(data_excel_file)

historical_Stock = final_merged1[
    (final_merged1['Price_Trend'].isin(['PosAboveATP'])) & (final_merged1['Star'].isin(['Morning Star', 'EOD Star']))]
Today_Stock_list = historical_Stock['Stock'].unique()
final_merged1.sort_values(by=['secWiseDelPosDate'],ascending=True, inplace=True) # desc
final_merged1['secWiseDelPosDate'] = final_merged1['secWiseDelPosDate'].dt.strftime('%m-%d-%Y %H:%M')


sheet_oi_single = wb.sheets("Working_Area")
sheet_oi_single.clear()
sheet_oi_single.range("A1").options(index=False).value = final_merged1
# final_merged1.to_csv(PRO)
wb.save()

if timenow > "15:40":
    wb.save()
    app = xw.apps.active
    app.quit()
