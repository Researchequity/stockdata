import numpy as np
import pandas as pd
from utils import *
from datetime import datetime

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

timenow = datetime.now().strftime('%H:%M')


def get_public_holding():
    df_base_pub = pd.read_csv(PROCESSED_DIR + '\\latest_base_shareholdings.csv')
    df_base_pub = df_base_pub[df_base_pub['Category of shareholder'] == '(B) Public']
    df_base_pub = df_base_pub[['Scrip_code', 'Company', 'Total no. shares held',
                               'Shareholding as a % of total no. of shares (calculated as per SCRR, 1957)As a % of (A+B+C2)']]
    df_base_pub.columns = ['Scrip_code', 'Company', 'public_shares_held', 'Total_pub_share_perct']

    df_Institution_pub = pd.read_csv(PROCESSED_DIR + '\\latest_detail_shareholdings.csv')

    df_Institution_pub = df_Institution_pub[df_Institution_pub['Nationality'] == 'Institutions']
    df_Institution_pub = df_Institution_pub[df_Institution_pub['Header'] == 1]
    df_Institution_pub_DII = df_Institution_pub[df_Institution_pub['Category'].isin(['Mutual Funds/'])]
    df_Institution_pub_DII = df_Institution_pub_DII[['Scrip_code', 'Total nos. shares held']]

    df_Institution_pub_FII = df_Institution_pub[df_Institution_pub['Category'].isin(['Foreign Portfolio Investors'])]
    df_Institution_pub_FII = df_Institution_pub_FII[['Scrip_code', 'Total nos. shares held']]
    df_Institution_pub = pd.merge(df_Institution_pub_DII, df_Institution_pub_FII, on=['Scrip_code'], how='inner')
    df_Institution_pub.columns = ['Scrip_code', 'DII_Holding', 'FII_holding']

    df_pub = pd.merge(df_base_pub, df_Institution_pub, on=['Scrip_code'], how='inner')

    df_pub['Total_pub_share_perct'] = pd.to_numeric(df_pub['Total_pub_share_perct'])
    df_pub['DII_perct'] = round(
        (df_pub['DII_Holding'] * 100 / df_pub['public_shares_held']) * df_pub['Total_pub_share_perct'] / 100, 2)
    df_pub['FII_perct'] = round(
        (df_pub['FII_holding'] * 100 / df_pub['public_shares_held']) * df_pub['Total_pub_share_perct'] / 100, 2)
    df_pub['pub_oth_perct'] = df_pub['Total_pub_share_perct'] - df_pub['FII_perct'] - df_pub['DII_perct']
    df_pub = df_pub[
        ['Scrip_code', 'public_shares_held', 'Total_pub_share_perct', 'DII_perct', 'FII_perct', 'pub_oth_perct']]

    # df_nse_bse_map = pd.read_csv(METADATA_DIR + '\\BSE_NSE_Code_mapping_using_ISIN.csv')[['SYMBOL', 'Scrip_code']]
    # df_nse_bse_map.columns = ['Stock', 'Scrip_code']
    # df_base_pub = pd.merge(df_pub, df_nse_bse_map, on=['Scrip_code'], how='inner')

    return df_pub


df_pub_share = get_public_holding()

is_trial = 0
timesleep = datetime.now().strftime('%H:%M')
is_data_dup = 1

delivery_position_df = pd.read_csv(PROCESSED_DIR + '\\nse_delivery_position.csv')

delivery_position_df = delivery_position_df[~(delivery_position_df['secWiseDelPosDate'].str[12:15] == 'EOD')]
metadata_df = pd.read_csv(METADATA_DIR + '\\StockMetadata.csv', usecols=['Symbol', 'companyName', 'MarketCap',
                                                                         'issued_shares', 'Sector', 'token', 'Industry',
                                                                         'script_code'])

average_df = pd.read_csv(METADATA_DIR + '\\Average.csv')
all_time_high_df = pd.read_csv(METADATA_DIR + '\\alltime52wkhigh.csv')
all_time_high_df = all_time_high_df.rename(columns={"52 Wk High Date": "52weekdate",
                                                    "52 Wk High Price": "52 week High price",
                                                    "all_time_high_Date": "allTimeHighDate",
                                                    "all_time_high_price": "High price all time"})
all_time_high_df = all_time_high_df[['Stock', 'High price all time', '52 week High price',
                                     'date_diff_frm_today_ath', 'date_diff_frm_today_52_week']]
all_time_high_df = all_time_high_df[all_time_high_df['Stock'].notna()]

merged = metadata_df.merge(average_df, how='inner', left_on='Symbol', right_on='Stock')

final_merged = delivery_position_df.merge(merged, how='inner', on='Stock')
final_merged.drop(['Symbol'], inplace=True, axis=1)

#  norm_mean is Delivery norm mean
final_merged['Times_of_norm_mean'] = final_merged['deliveryQuantity'].div(final_merged['del_norm_mean']).replace(np.inf,
                                                                                                                 0)
final_merged['Times_of_norm_mean'] = final_merged['Times_of_norm_mean'].round(1)
final_merged['Times_of_mean_dQty'] = final_merged['deliveryQuantity'].div(final_merged['del_norm_mean']).replace(np.inf,
                                                                                                                 0)
final_merged['Times_of_mean_dQty'] = final_merged['Times_of_mean_dQty'].round(1)
# final_merged['Greater_than_Avg']=np.where((final_merged['deliveryQuantity']>final_merged['norm_mean']),True,False)
final_merged['Price_Trend'] = np.where(final_merged.pChange >= 0, (
    np.where(final_merged.lastPrice >= final_merged.vwap, 'PosAboveATP', 'Positive')), (
                                           np.where(final_merged.lastPrice <= final_merged.vwap, 'NegBelowATP',
                                                    'Negative')))
final_merged['Vbull'] = np.where(final_merged.Price_Trend == 'PosAboveATP', (
    np.where(((final_merged.deliveryQuantity) / (final_merged.del_norm_mean)) > 0.7,
             (np.where(((final_merged.deliveryQuantity) >= (final_merged.max_dQty)), 1, np.nan)), np.nan)), np.nan)
final_merged['Vbull'] = final_merged['Vbull'].replace(np.inf, 0)
final_merged['Morning'] = np.where(final_merged.MarketCap == 'LARGE', 1.5, (np.where(final_merged.MarketCap == 'Mid', 2,
                                                                                     (np.where(((
                                                                                                            final_merged.MarketCap == 'Small') | (
                                                                                                            final_merged.MarketCap == 'VSM')),
                                                                                               3, 1)))))
final_merged['Evening'] = np.where(final_merged.MarketCap == 'LARGE', 2, (np.where(final_merged.MarketCap == 'Mid', 3, (
    np.where(((final_merged.MarketCap == 'Small') | (final_merged.MarketCap == 'VSM')), 5, 2)))))
final_merged['Above_20D_avg'] = 0  # np.where((final_merged['20DMA_price']<final_merged['lastPrice']),True,False)
final_merged['combined1'] = final_merged['deliveryQuantity'] * final_merged['lastPrice']
final_merged['combined1'] = final_merged['combined1'].astype(float)
final_merged['combined'] = final_merged['combined1'].round(1)
final_merged['Abv_30L'] = np.where(final_merged.combined > 3000000, 1, 0)

final_merged['secWiseDelPosDate'] = pd.to_datetime(final_merged['secWiseDelPosDate'])

final_merged['time'] = final_merged['secWiseDelPosDate'].dt.time

final_merged1 = final_merged.dropna(subset=['time'])
final_merged1['is_evening'] = np.where(final_merged1['time'] < pd.to_datetime('12:00:00').time(), 0, 1)

final_merged1['Star'] = np.where(final_merged1['is_evening'] == 0, (
    np.where(((final_merged1['Times_of_norm_mean']) > (final_merged1['Morning'])), 'Morning Star', '')), (
                                     np.where(((final_merged1['Times_of_norm_mean']) > (final_merged1['Evening'])),
                                              'EOD Star', '')))
final_merged1 = final_merged1.drop(['combined1', 'combined', 'time'], axis=1)

final_merged1['20DMA_price'] = 0
final_merged1['norm_mean'] = final_merged1['del_norm_mean']
final_merged1 = final_merged1[
    ['Stock', 'secWiseDelPosDate', 'deliveryQuantity', 'quantityTraded', 'deliveryToTradedQuantity', 'lastPrice',
     'pChange', 'vwap', 'companyName', 'MarketCap', 'issued_shares', 'Times_of_mean_dQty', '2time_20day', '3time_20day',
     '2time_3day',
     '20DMA_price', 'norm_mean', 'max_dQty', 'Times_of_norm_mean', 'Price_Trend', 'Vbull',
     'Morning', 'Evening', 'Above_20D_avg', 'Abv_30L', 'Star', 'Sector', 'Industry', 'token', 'script_code']]

final_merged1['issued_shares'] = final_merged1['issued_shares'].replace("-", "-1")
final_merged1['del_perct_issued_shares'] = round(
    (final_merged1['deliveryQuantity'] / final_merged1['issued_shares'].astype(float)) * 100, 1)
final_merged1['del_perct_isud_gt_1'] = np.where(final_merged1['del_perct_issued_shares'] >= 0.5, 0.5, 0)
final_merged1['del_perct_isud_gt_1'] = np.where(final_merged1['del_perct_issued_shares'] >= 1, 1,
                                                final_merged1['del_perct_isud_gt_1'])
final_merged1['del_perct_isud_gt_1'] = np.where(final_merged1['del_perct_issued_shares'] >= 2, 2,
                                                final_merged1['del_perct_isud_gt_1'])

# All time high and 52 week dataframes
final_merged1 = pd.merge(final_merged1, all_time_high_df, on=['Stock'], how='left')
final_merged1['p_chng_52'] = round(
    (final_merged1['lastPrice'] - final_merged1['52 week High price']) * 100 / final_merged1['lastPrice'], 1)
final_merged1['p_chng_ath'] = round(
    (final_merged1['lastPrice'] - final_merged1['High price all time']) * 100 / final_merged1['lastPrice'], 1)

final_merged1['is_price_near_52_ath'] = np.where(final_merged1['p_chng_52'] > -5.0, 52, 0)
final_merged1['is_price_near_52_ath'] = np.where(final_merged1['p_chng_ath'] > -5.0, 100,
                                                 final_merged1['is_price_near_52_ath'])
final_merged1.drop(['High price all time', '52 week High price'], inplace=True, axis=1)

final_merged1['flag_ath_date_diff_frm_today'] = np.where(
    (final_merged1['date_diff_frm_today_ath'] >= 90) & (final_merged1['p_chng_ath'] > -5), '90_180days', 0)
final_merged1['flag_ath_date_diff_frm_today'] = np.where(
    (final_merged1['date_diff_frm_today_ath'] > 180) & (final_merged1['p_chng_ath'] > -5), '181_360days',
    final_merged1['flag_ath_date_diff_frm_today'])
final_merged1['flag_ath_date_diff_frm_today'] = np.where(
    (final_merged1['date_diff_frm_today_ath'] > 360) & (final_merged1['p_chng_ath'] > -5), '361_1000days',
    final_merged1['flag_ath_date_diff_frm_today'])
final_merged1['flag_ath_date_diff_frm_today'] = np.where(
    (final_merged1['date_diff_frm_today_ath'] > 1000) & (final_merged1['p_chng_ath'] > -5), '1001_abvdays',
    final_merged1['flag_ath_date_diff_frm_today'])

final_merged1['flag_52w_date_diff_frm_today'] = np.where(
    (final_merged1['date_diff_frm_today_52_week'] >= 90) & (final_merged1['p_chng_52'] > -5), '90_180days', 0)
final_merged1['flag_52w_date_diff_frm_today'] = np.where(
    (final_merged1['date_diff_frm_today_52_week'] > 180) & (final_merged1['p_chng_52'] > -5), '181_360days',
    final_merged1['flag_52w_date_diff_frm_today'])
final_merged1['flag_52w_date_diff_frm_today'] = np.where(
    (final_merged1['date_diff_frm_today_52_week'] > 360) & (final_merged1['p_chng_52'] > -5), '361_1000days',
    final_merged1['flag_52w_date_diff_frm_today'])
final_merged1['flag_52w_date_diff_frm_today'] = np.where(
    (final_merged1['date_diff_frm_today_52_week'] > 1000) & (final_merged1['p_chng_52'] > -5), '1001_abvdays',
    final_merged1['flag_52w_date_diff_frm_today'])

stockmetadata = pd.read_csv(METADATA_DIR + '\\stockmetadata_nse_fut.csv')
stockmetadata['EXPIRY_DT'] = pd.to_datetime(stockmetadata['EXPIRY_DT'], format='%d-%m-%Y %H:%M')
stockmetadata_unique = stockmetadata['SYMBOL'].unique()
final_merged1['is_FNO_stock'] = final_merged1['Stock'].isin(stockmetadata_unique)
final_merged1["is_FNO_stock"] = final_merged1["is_FNO_stock"].astype(int)

# merge price band
band_df = pd.read_csv(METADATA_DIR + '\\sec_list.csv')
band_df = band_df.rename(columns={"Symbol": "Stock"})
band_df.drop(['Series', 'Security Name'], inplace=True, axis=1)
final_merged1 = pd.merge(final_merged1, band_df, on=['Stock'], how='left')

# merge calender_results
calendar_results = pd.read_csv(METADATA_DIR + '\\results_calendar.csv')[['Security Name', 'Result Date']]
calendar_results = calendar_results.rename(columns={"Security Name": "Stock"})
final_merged1 = pd.merge(final_merged1, calendar_results, on=['Stock'], how='left')
# date = ''.join(str(datetime.today().date()).split('-'))


date = datetime.now().strftime("%Y%m%d")

# share holding pattern
final_merged1 = pd.merge(final_merged1, df_pub_share, left_on=['script_code'], right_on=['Scrip_code'], how='left')
final_merged1.drop(['script_code'], inplace=True, axis=1)
final_merged1['del_pub_perct'] = round(final_merged1['deliveryQuantity'] * 100 / final_merged1['public_shares_held'], 1)
final_merged1['equity_pub_change_2'] = np.where(final_merged1['del_pub_perct'] > 2, 1, 0)


historical_Stock = final_merged1[
    (final_merged1['Price_Trend'].isin(['PosAboveATP'])) & (final_merged1['Star'].isin(['Morning Star', 'EOD Star']))]
Today_Stock_list = historical_Stock['Stock'].unique()

# Historical view of Delivery
workingarea = PROCESSED_DIR + "\\working_area_NSE\\"
Working_df_all = pd.DataFrame()
from glob import glob

for file in glob(workingarea + 'Working_Area_*.csv'):
    # print(file)
    Working_df = pd.read_csv(file)
    Working_df_all = pd.concat([Working_df_all, Working_df])

Working_df_all['secWiseDelPosDate'] = pd.to_datetime(Working_df_all['secWiseDelPosDate'])
Working_df_all['time'] = Working_df_all['secWiseDelPosDate'].dt.strftime('%H:%M')

today_max = Working_df_all['secWiseDelPosDate'].max()

Working_df_all = Working_df_all[
    (Working_df_all['time'] == '16:00') | (Working_df_all['secWiseDelPosDate'] == today_max)]

all_date = Working_df_all['secWiseDelPosDate'].unique()  # ['secWiseDelPosDate']
date_list = all_date[-10:]

Working_df_all = Working_df_all[
    Working_df_all['Stock'].isin(Today_Stock_list) & Working_df_all['secWiseDelPosDate'].isin(date_list)]
Working_df_all['secWiseDelPosDate'] = Working_df_all['secWiseDelPosDate'].dt.strftime('%Y-%m-%d')

import xlwings as xw

#data_excel_file = r"D:\Program\python_ankit\Equity_Live_Data.xlsm"
#wb = xw.Book(data_excel_file)

#sheet_oi_single = wb.sheets("Historical_del_data")
#sheet_oi_single.range("A1").options(index=False).value = Working_df_all

# ##Dump Output in Excel
final_merged1['secWiseDelPosDate'] = pd.to_datetime(final_merged1['secWiseDelPosDate'], format='%d-%m-%Y %H:%M')
final_merged1['secWiseDelPosDate'] = final_merged1['secWiseDelPosDate'].dt.strftime('%d-%m-%Y %H:%M')
final_merged1.to_csv(PROCESSED_DIR + "\\working_area_NSE" + "\\Working_Area_{}.csv".format(date), index=False)

#sheet_oi_single = wb.sheets("Working_Area")
#sheet_oi_single.range("A1").options(index=False).value = final_merged1
#wb.save()

##
# xw.Book(r"D:\Program\Data\NSE_Equity_Data.xlsm").sheets("Working_Area").range("A1").options(index=False).value =final_merged1
# xw.Book(r"D:\Program\Data\NSE_Equity_Data.xlsm").save()

if timenow > "15:40":
    wb.save()
    wb.close()
