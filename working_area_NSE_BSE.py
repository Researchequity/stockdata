import pandas as pd
from datetime import datetime as dt
from utils import *
import time
start_time = time.time()

#code updated
date_today = datetime.date.today() #- datetime.timedelta(days=1)
date_today, prev_date, fut_date = get_market_prev_date_fut_date(date_today)

date_today = date_today.strftime('%d%m%y')

timenow = dt.now().strftime('%H:%M')

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

    return df_pub


df_pub_share = get_public_holding()

# read files
df_bse = pd.read_csv(PROCESSED_DIR + '\\del_data_BSE' + '\\bse_delivery_data_' + date_today + '.csv')
# df_bse = df_bse[df_bse.SC_CODE == 532541]
df_bse = df_bse[df_bse['pChange'] != '-']
df_bse['secWiseDelPosDate'] = pd.to_datetime(df_bse['secWiseDelPosDate'], format='%d-%m-%Y %H:%M')
df_bse = df_bse[df_bse['secWiseDelPosDate'] == df_bse['secWiseDelPosDate'].max()]
df_bse['secWiseDelPosDate'] = df_bse['secWiseDelPosDate'].dt.strftime('%d-%m-%Y %H')
df_bse['secWiseDelPosDate'] = pd.to_datetime(df_bse['secWiseDelPosDate'], format='%d-%m-%Y %H')
df_bse['vwap'] = df_bse['vwap'].str.replace(',', '').astype(float)
df_bse.rename(columns={'SC_CODE':'script_code'},inplace = True)
df_bse.drop('Stock',axis=1, inplace =True)

df_nse = pd.read_csv(PROCESSED_DIR + '\\del_data_NSE' + '\\nse_delivery_position_' + date_today + '.csv')
# df_nse = df_nse[df_nse.Stock == 'COFORGE']

df_nse['secWiseDelPosDate'] = pd.to_datetime(df_nse['secWiseDelPosDate'], format='%d-%m-%Y %H:%M')

df_bse_avg = pd.read_csv(METADATA_DIR + '\\Average_bse.csv')[['SCRIP CODE', 'DEL_norm_mean']]
df_bse_avg = df_bse_avg.rename(columns={"SCRIP CODE": "script_code", "DEL_norm_mean": "del_norm_mean"})
df_nse_avg = pd.read_csv(METADATA_DIR + '\\Average.csv')[['Stock', 'del_norm_mean','max_dQty']]

df_stock_metadata = pd.read_csv(METADATA_DIR + '\\StockMetadata.csv',
                                usecols=['Symbol', 'companyName', 'MarketCap', 'issued_shares', 'Sector', 'token',
                                         'Industry', 'script_code', 'ISIN','surveillance'])
df_stock_metadata.rename(columns={'Symbol':'Stock'},inplace = True)

# merge nse,bse with stockmetadata
df_nse_merge = pd.merge(df_nse, df_stock_metadata, how='inner', on='Stock')
df_bse_merge = pd.merge(df_bse, df_stock_metadata, how='inner', on='script_code')
df_bse_merge['Stock'] = ''
# merge with average files
df_nse_merge = pd.merge(df_nse_merge, df_nse_avg, how='left', on='Stock')
df_bse_merge = pd.merge(df_bse_merge, df_bse_avg, how='left', on='script_code')

df_nse_merge['source'] = 'NSE'
df_bse_merge['source'] = 'BSE'
df_bse_merge['source'] = np.where(df_bse_merge['vwap']<=20,'BSE20','BSE')

# rearranging columns of nse as per of bse
df_nse_merge = df_nse_merge[["Stock", "secWiseDelPosDate", "deliveryQuantity", "quantityTraded", "deliveryToTradedQuantity","lastPrice",
                              "pChange", "vwap", "companyName",  "MarketCap", "issued_shares","Sector","Industry", "token", "Series", "ISIN", "script_code",
                             "del_norm_mean",'max_dQty', "source","surveillance"]]

# concat merged bse,nse df
final_merged = pd.concat([df_nse_merge,df_bse_merge], axis=0, ignore_index=True)

#  norm_mean is Delivery norm mean
final_merged['Times_of_mean_dQty'] = final_merged['deliveryQuantity'].div(final_merged['del_norm_mean']).replace(np.inf,0)
final_merged['Times_of_mean_dQty'] = final_merged['Times_of_mean_dQty'].round(1)

final_merged['lastPrice']=final_merged['lastPrice'].astype(float)
final_merged['pChange']=final_merged['pChange'].astype(float)
final_merged['Price_Trend']=np.where(final_merged['pChange'] >= 0,
                                     (np.where(final_merged['lastPrice']>=final_merged['vwap'],'PosAboveATP','Positive')),
                                     (np.where(final_merged['lastPrice']<=final_merged['vwap'],'NegBelowATP','Negative')))

# we dont have 'max_dQty' column in bse average file so how to calculate Vbull
final_merged['Vbull']= np.where(final_merged.Price_Trend == 'PosAboveATP',(np.where(((final_merged.deliveryQuantity) / (final_merged.del_norm_mean))>0.7,(np.where(((final_merged.deliveryQuantity) >= (final_merged.max_dQty)),1,np.nan)),np.nan)),np.nan)
final_merged['Vbull']= final_merged['Vbull'].replace(np.inf,0)
final_merged['Morning'] = np.where(final_merged.MarketCap=='LARGE',1.5,(np.where(final_merged.MarketCap=='Mid',2,(np.where(((final_merged.MarketCap=='Small') | (final_merged.MarketCap=='VSM')),3,1)))))
final_merged['Evening'] = np.where(final_merged.MarketCap=='LARGE',2,(np.where(final_merged.MarketCap=='Mid',3,(np.where(((final_merged.MarketCap=='Small') | (final_merged.MarketCap=='VSM')),5,2)))))
final_merged['Above_20D_avg']= 0 #np.where((final_merged['20DMA_price']<final_merged['lastPrice']),True,False)
final_merged['combined1']=final_merged['deliveryQuantity']*final_merged['lastPrice']
final_merged['combined1']=final_merged['combined1'].astype(float)
final_merged['combined'] = final_merged['combined1'].round(1)
final_merged['Abv_30L'] = np.where(final_merged.combined > 1000000,1,0)

final_merged['secWiseDelPosDate'] = pd.to_datetime(final_merged['secWiseDelPosDate'], format='%d-%m-%Y %H:%M')
final_merged['time']=final_merged['secWiseDelPosDate'].dt.time
final_merged1 = final_merged.dropna(subset=['time'])
final_merged1['is_evening']=np.where(final_merged1['time'] < pd.to_datetime('12:00:00').time(),0,1)
final_merged1['Star']=np.where(final_merged1['is_evening']==0,(np.where(((final_merged1['Times_of_mean_dQty'])>(final_merged1['Morning'])),'Morning Star','')),(np.where(((final_merged1['Times_of_mean_dQty'])>(final_merged1['Evening'])),'EOD Star','')))
final_merged1=final_merged1.drop(['combined1','combined','time'],axis=1)
final_merged1['20DMA_price'] = 0
final_merged1['norm_mean'] = final_merged1['del_norm_mean']

final_merged1 = final_merged1[['Stock','secWiseDelPosDate','deliveryQuantity','quantityTraded','deliveryToTradedQuantity','lastPrice',
                               'pChange','vwap','companyName','MarketCap','issued_shares',"Sector","Industry", "token", "Series", "ISIN", "script_code", "source",
                               'Times_of_mean_dQty','norm_mean','max_dQty','Price_Trend','Vbull',
                               'Morning','Evening',	'Above_20D_avg','Abv_30L','Star']]

final_merged1['issued_shares'] = final_merged1['issued_shares'].replace("-", "-1")
final_merged1['del_perct_issued_shares'] = round((final_merged1['deliveryQuantity'] / final_merged1['issued_shares'].astype(float))*100,1)
final_merged1['del_perct_isud_gt_1']= np.where(final_merged1['del_perct_issued_shares']>=0.5,0.5,0)
final_merged1['del_perct_isud_gt_1']= np.where(final_merged1['del_perct_issued_shares']>=1,1,final_merged1['del_perct_isud_gt_1'])
final_merged1['del_perct_isud_gt_1']= np.where(final_merged1['del_perct_issued_shares']>=2,2,final_merged1['del_perct_isud_gt_1'])

#All time high and 52 week dataframes
all_time_high_df = pd.read_csv(METADATA_DIR + '\\alltime52wkhigh.csv')
all_time_high_df = all_time_high_df.rename(columns={"52 Wk High Date": "52weekdate",
                                                    "52 Wk High Price": "52 week High price",
                                                    "all_time_high_Date": "allTimeHighDate",
                                                    "all_time_high_price": "High price all time"})
all_time_high_df = all_time_high_df[['Security Code', 'Stock', '52weekdate',
       '52 week High price', 'allTimeHighDate', 'High price all time',
       'date_diff_frm_today_ath', 'date_diff_frm_today_52_week']]
all_time_high_nse = all_time_high_df[all_time_high_df['Stock'].notna()]
all_time_high_bse = all_time_high_df[all_time_high_df['Security Code'].notna()]
final_merged_nse = final_merged1[final_merged1.source == 'NSE']
final_merged_bse = final_merged1[final_merged1.source == 'BSE']
# to include new listings changed inner to left
final_merged_nse = pd.merge(final_merged_nse, all_time_high_nse, on='Stock', how='left')
final_merged_bse = pd.merge(final_merged_bse, all_time_high_bse, how='left', left_on='script_code', right_on='Security Code')
final_merged1 = pd.concat([final_merged_nse, final_merged_bse])
final_merged1.drop(['Stock_x', 'Stock_y'], inplace=True, axis=1)


final_merged1['p_chng_52'] = round((final_merged1['lastPrice'] - final_merged1['52 week High price'])* 100 / final_merged1['lastPrice'],1)
final_merged1['p_chng_ath'] = round((final_merged1['lastPrice'] - final_merged1['High price all time'])* 100 / final_merged1['lastPrice'],1)
final_merged1['is_price_near_52_ath'] = np.where(final_merged1['p_chng_52'] > -5.0,52, 0)
final_merged1['is_price_near_52_ath'] = np.where(final_merged1['p_chng_ath'] > -5.0,100,final_merged1['is_price_near_52_ath'])
final_merged1.drop(['High price all time','52 week High price'], inplace=True, axis=1)

final_merged1['flag_ath_date_diff_frm_today'] =np.where((final_merged1['date_diff_frm_today_ath'] >=90) & (final_merged1['p_chng_ath']>-5),'90_180days', 0)
final_merged1['flag_ath_date_diff_frm_today'] =np.where((final_merged1['date_diff_frm_today_ath'] >180) & (final_merged1['p_chng_ath']>-5), '181_360days', final_merged1['flag_ath_date_diff_frm_today'])
final_merged1['flag_ath_date_diff_frm_today'] =np.where((final_merged1['date_diff_frm_today_ath'] >360) & (final_merged1['p_chng_ath']>-5), '361_1000days', final_merged1['flag_ath_date_diff_frm_today'])
final_merged1['flag_ath_date_diff_frm_today'] =np.where((final_merged1['date_diff_frm_today_ath'] >1000) & (final_merged1['p_chng_ath']>-5),'1001_abvdays', final_merged1['flag_ath_date_diff_frm_today'])

final_merged1['flag_52w_date_diff_frm_today'] =np.where((final_merged1['date_diff_frm_today_52_week'] >=90) & (final_merged1['p_chng_52']>-5),'90_180days', 0)
final_merged1['flag_52w_date_diff_frm_today'] =np.where((final_merged1['date_diff_frm_today_52_week'] >180) & (final_merged1['p_chng_52']>-5),'181_360days', final_merged1['flag_52w_date_diff_frm_today'])
final_merged1['flag_52w_date_diff_frm_today'] =np.where((final_merged1['date_diff_frm_today_52_week'] >360) & (final_merged1['p_chng_52']>-5),'361_1000days', final_merged1['flag_52w_date_diff_frm_today'])
final_merged1['flag_52w_date_diff_frm_today'] =np.where((final_merged1['date_diff_frm_today_52_week'] >1000) & (final_merged1['p_chng_52']>-5),'1001_abvdays', final_merged1['flag_52w_date_diff_frm_today'])

stockmetadata = pd.read_csv(METADATA_DIR + '\\stockmetadata_nse_fut.csv')
stockmetadata['EXPIRY_DT'] = pd.to_datetime(stockmetadata['EXPIRY_DT'], format='%d-%m-%Y %H:%M')
stockmetadata_unique = stockmetadata['SYMBOL'].unique()
final_merged1['is_FNO_stock'] = final_merged1['Stock'].isin(stockmetadata_unique)
final_merged1["is_FNO_stock"] = final_merged1["is_FNO_stock"].astype(int)

# merge price band
band_df = pd.read_csv(METADATA_DIR + '\\sec_list.csv')
band_df = band_df.rename(columns={"Symbol": "Stock"})
band_df.drop(['Series','Security Name'], inplace=True, axis=1)
final_merged1 = pd.merge(final_merged1,band_df, on=['Stock'], how='left')


#merge calender_results
calendar_results = pd.read_csv(METADATA_DIR + '\\results_calendar.csv')[['Security Name', 'Result Date']]
calendar_results = calendar_results.rename(columns={"Security Name": "Stock"})
final_merged1 = pd.merge(final_merged1,calendar_results, on=['Stock'], how='left')
date = dt.now().strftime("%Y%m%d")

# share holding pattern
final_merged1 = pd.merge(final_merged1,df_pub_share,left_on=['script_code'],right_on=['Scrip_code'],how='left')
final_merged1.drop(['Scrip_code'], inplace=True, axis=1)
final_merged1['del_pub_perct'] = round(final_merged1['deliveryQuantity']*100/final_merged1['public_shares_held'],1)
final_merged1['equity_pub_change_2'] = np.where(final_merged1['del_pub_perct']>2,1,0)
final_merged1["Stock"].fillna(final_merged1["script_code"], inplace=True)

import xlwings as xw
# data_excel_file = r"D:\Program\stockdata\shared_cloud\Equity_Live_Data_NSE_BSE.xlsm"
data_excel_file = r"D:\Program\python_ankit\Equity_Live_Data_NSE_BSE.xlsm"
wb = xw.Book(data_excel_file)

historical_Stock = final_merged1[ (final_merged1['Price_Trend'].isin(['PosAboveATP'])) & (final_merged1['Star'].isin(['Morning Star','EOD Star']))]
Today_Stock_list = historical_Stock['Stock'].unique()

# Historical view of Delivery
workingarea = PROCESSED_DIR + "\\working_area\\"
Working_df_all = pd.DataFrame()
from glob import glob
for file in glob(workingarea + 'Working_Area_*.csv'):
    Working_df = pd.read_csv(file)
    Working_df_all = pd.concat([Working_df_all, Working_df])


Working_df_all['secWiseDelPosDate'] = pd.to_datetime(Working_df_all['secWiseDelPosDate'])
Working_df_all['time'] = Working_df_all['secWiseDelPosDate'].dt.strftime('%H:%M')

today_max = Working_df_all['secWiseDelPosDate'].max()

Working_df_all = Working_df_all[(Working_df_all['time'] == '16:00')| (Working_df_all['secWiseDelPosDate']==today_max)]

all_date = Working_df_all['secWiseDelPosDate'].unique()#['secWiseDelPosDate']
date_list = all_date[-10:]

Working_df_all = Working_df_all[Working_df_all['Stock'].isin(Today_Stock_list) & Working_df_all['secWiseDelPosDate'].isin(date_list)]
Working_df_all['secWiseDelPosDate'] = Working_df_all['secWiseDelPosDate'].dt.strftime('%Y-%m-%d')


sheet_oi_single = wb.sheets("Historical_del_data")
sheet_oi_single.range("A1").options(index=False).value =Working_df_all


sma_20 = pd.read_csv(METADATA_DIR + '\\sma_20_nse_bse.csv')[['Stock', 'ratio_20ma', 'is_ltp_greater','20_ma', 'ratio_20ma_vol']]
final_merged1 = pd.merge(final_merged1, sma_20, on=['Stock'], how='left')

final_merged1['ltp_to_20_ma'] = (((final_merged1['lastPrice'] - final_merged1['20_ma']) / final_merged1['20_ma'])*100).round(2)

sheet_oi_single = wb.sheets("Working_Area")
sheet_oi_single.clear()
sheet_oi_single.range("A1").options(index=False).value =final_merged1

###Dump Output in Excel
final_merged1['secWiseDelPosDate'] = final_merged1['secWiseDelPosDate'].dt.strftime('%d-%m-%Y %H:%M')
final_merged1.to_csv(PROCESSED_DIR + "\\working_area" + "\\Working_Area_{}.csv".format(date), index=False)

# dump market strength
sheet_oi_single = wb.sheets("Market_Tracker")
sheet_oi_single.clear()#.range('A4:F10').clear()
mrkt_strength = pd.read_csv(RAW_DIR + '\\mrkt_strength.csv')
mrkt_strength = mrkt_strength.rename(columns={"temp_time": "Time", "strength_historical": "hist_val_in_cr",
                              "strength_today_sum": "today_val_in_cr"})
mrkt_strength['hist_val_in_cr'] = (mrkt_strength['hist_val_in_cr'] / 10000000).round(2)
mrkt_strength['today_val_in_cr'] = (mrkt_strength['today_val_in_cr'] / 10000000).round(2)
mrkt_strength['perct'] = mrkt_strength['perct'].round(2)
mrkt_strength = mrkt_strength[["Time", "hist_val_in_cr", "today_val_in_cr", "perct", "stock_count_today"]]
sheet_oi_single.range("B3").options(index=False).value = mrkt_strength
pchng = pd.read_csv(RAW_DIR + '\\mrkt_pchng_count.csv')[['pos_stock', 'neg_stock']]
sheet_oi_single.range("G3").options(index=False).value = pchng
wb.save()


if timenow > "15:40":
    wb.save()
    wb.close()
