import time
import mimetypes
import requests
from bse_shareholdings import *
import win32com.client
from dateutil.relativedelta import relativedelta
from email.utils import make_msgid
from datetime import datetime as dt
from utils import *
import urllib
from urllib.request import urlopen
import dataframe_image as dfi

file = os.path.basename(__file__)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

date_today = datetime.date.today()  # - datetime.timedelta(days=1)
date_today, prev_date, fut_date = get_market_prev_date_fut_date(date_today)

TODAY_DATE = datetime.date.today()  #- datetime.timedelta(days=1)
data_excel_file = REPORT_DIR + "\\intermedent.xlsm"
wb = xw.Book(data_excel_file)


# wb = xw.Book(REPORT_DIR + "\\intermedent.xlsm")


def week_high():
    df = pd.read_csv(PROCESSED_DIR+'\\'+'Historical_52_Wk_High.csv')

    df['Date'] = pd.to_datetime(df['Date'], format='%d-%m-%Y')
    df.sort_values(by=['Date'], inplace=True, ascending=True)

    df = df[df['Date'] >= (pd.datetime.today() - pd.Timedelta(days=90)).strftime("%d-%m-%Y")]

    data = []
    sript_name_list = df['Security Name'].unique().tolist()
    for sn in sript_name_list:
        merge_sn = df[df['Security Name'] == sn]

        merge_sn['Date'] = pd.to_datetime(merge_sn['Date'], format='%d-%m-%Y')
        merge_sn.sort_values(by=['Date'], inplace=True, ascending=True)

        Script_Code = merge_sn['Security Code'].tail(1).tolist()
        Latest_52_Wk_High_Date = merge_sn['Date'].tail(1).tolist()

        Last30_Days = merge_sn[merge_sn['Date'] >= (pd.datetime.today() - pd.Timedelta(days=30)).strftime("%d-%m-%Y")]
        Last30_Days = len(Last30_Days['Security Name'])

        Last60_Days = merge_sn[merge_sn['Date'] >= (pd.datetime.today() - pd.Timedelta(days=60)).strftime("%d-%m-%Y")]
        Last60_Days = len(Last60_Days['Security Name'])

        Last90_Days = merge_sn[merge_sn['Date'] >= (pd.datetime.today() - pd.Timedelta(days=90)).strftime("%d-%m-%Y")]
        Last90_Days = len(Last90_Days['Security Name'])

        data.append([Script_Code[0], sn, Latest_52_Wk_High_Date[0], Last30_Days, Last60_Days, Last90_Days])
    table_df_csv = pd.DataFrame(data)
    table_df_csv = pd.DataFrame(table_df_csv.values.reshape(-1, 6),
                                columns=['script_code','Security Name', 'Latest 52 Weeks High Date', 'Last30_Days', 'Last60_Days',
                                         'Last90_Days'])

    StockMetadata = pd.read_csv(METADATA_DIR+'\\'+'StockMetadata.csv')[['script_code', 'MarketCap', 'totalMarketCap']]

    StockMetadata['script_code']=StockMetadata['script_code'].astype('object')
    merge = pd.merge(table_df_csv, StockMetadata, on=['script_code'], how='left')


    workbook = xw.Book(REPORT_DIR+'\\'+'intermedent.xlsm')
    ws1 = workbook.sheets['52_Wk_High_Data_Summary']
    ws1.clear()
    ws1.range("A1").options(index=None).value = merge


def count_announcement():
    df = pd.read_csv(METADATA_DIR+'\\'+'Historical_Announcement.csv')
    df = df.drop_duplicates()

    Pattern = re.compile('Investor Meet - Intimation')
    df = df[df['name'].str.contains(Pattern)]

    qtr = []
    df['Exchange_Received'] = pd.to_datetime(df['Exchange_Received'], format='%d-%m-%Y %H:%M')
    df.sort_values(by=['Exchange_Received'], inplace=True, ascending=False)

    for val in df['Exchange_Received']:
        quarter = (val.month - 1) // 3 + 1
        if quarter == 4:
            Qtr_date = datetime.datetime(val.year, 12, 31)
            qtr_mon = Qtr_date.strftime('%B')
            qtr_year = Qtr_date.year
            str_date = str(qtr_mon) + ' ' + str(qtr_year)
            qtr.append(str_date)
            qtr.append(val)
        elif quarter == 1:
            Qtr_date = datetime.datetime(val.year, 3, 31)
            qtr_mon = Qtr_date.strftime('%B')
            qtr_year = Qtr_date.year
            str_date = str(qtr_mon) + ' ' + str(qtr_year)
            qtr.append(str_date)
            qtr.append(val)
        elif quarter == 2:
            Qtr_date = datetime.datetime(val.year, 6, 30)
            qtr_mon = Qtr_date.strftime('%B')
            qtr_year = Qtr_date.year
            str_date = str(qtr_mon) + ' ' + str(qtr_year)
            qtr.append(str_date)
            qtr.append(val)
        else:
            Qtr_date = datetime.datetime(val.year, 9, 30)
            qtr_mon = Qtr_date.strftime('%B')
            qtr_year = Qtr_date.year
            str_date = str(qtr_mon) + ' ' + str(qtr_year)
            qtr.append(str_date)
            qtr.append(val)

    qtr_df = pd.DataFrame(qtr)
    qtr_df = pd.DataFrame(qtr_df.values.reshape(-1, 2), columns=['qtr', 'Reported_toExchange_Date'])
    qtr_df = (qtr_df.drop_duplicates(subset=['qtr', 'Reported_toExchange_Date']))

    qtr_df = qtr_df[['Reported_toExchange_Date', 'qtr']]
    qtr_df.rename(columns={'Reported_toExchange_Date': 'Exchange_Received'}, inplace=True)
    qtr_df = pd.merge(qtr_df, df, on=['Exchange_Received'], how='left')

    qtrdata = qtr_df['qtr'].unique().tolist()
    df_merge = pd.DataFrame()
    for q in range(0, len(qtrdata)):
        data = qtr_df[qtr_df['name'].str.contains(Pattern) & qtr_df['qtr'].str.contains(qtrdata[q])]
        group = data.groupby(['name', 'script_code', 'qtr']).size().sort_values(ascending=False).reset_index(
            name='count')
        df_merge = pd.concat([df_merge, group], ignore_index=True)

    pivot = pd.pivot_table(df_merge, index=['name', 'script_code'], columns=['qtr'], values=['count'], aggfunc=np.sum)

    pivot.columns = pivot.columns.droplevel(0)
    pivot = pivot.reset_index().rename_axis(None, axis=1)

    qtrdata.insert(0, 'script_code')
    qtrdata.insert(0, 'name')

    pivot = pivot[qtrdata]

    AVG = pivot[pivot.columns[4:len(pivot.columns)]]
    pivot['Ratio_Avg'] = pivot[pivot.columns[2]] / (AVG.mean(axis=1))

    workbook = xw.Book(REPORT_DIR+'\\'+'intermedent.xlsm')
    ws1 = workbook.sheets['Count_Investor_Meet']
    ws1.clear()
    ws1.range('A1').options(index=False).value = pivot
    workbook.save()


def investor_meet_by_participant():
    df = pd.read_csv(RAW_DIR +'\\'+'Invester_Meet_No_Participant.csv')
    df = df.drop_duplicates()

    df['Number_Of_Participat'] = df['Number_Of_Participat'].replace(0, 1)

    qtr = []
    df['Exchange_Received'] = pd.to_datetime(df['Exchange_Received'], format='%d-%m-%Y')
    df.sort_values(by=['Exchange_Received'], inplace=True, ascending=False)

    for val in df['Exchange_Received']:
        quarter = (val.month - 1) // 3 + 1
        if quarter == 4:
            Qtr_date = datetime.datetime(val.year, 12, 31)
            qtr_mon = Qtr_date.strftime('%B')
            qtr_year = Qtr_date.year
            str_date = str(qtr_mon) + ' ' + str(qtr_year)
            qtr.append(str_date)
            qtr.append(val)
        elif quarter == 1:
            Qtr_date = datetime.datetime(val.year, 3, 31)
            qtr_mon = Qtr_date.strftime('%B')
            qtr_year = Qtr_date.year
            str_date = str(qtr_mon) + ' ' + str(qtr_year)
            qtr.append(str_date)
            qtr.append(val)
        elif quarter == 2:
            Qtr_date = datetime.datetime(val.year, 6, 30)
            qtr_mon = Qtr_date.strftime('%B')
            qtr_year = Qtr_date.year
            str_date = str(qtr_mon) + ' ' + str(qtr_year)
            qtr.append(str_date)
            qtr.append(val)
        else:
            Qtr_date = datetime.datetime(val.year, 9, 30)
            qtr_mon = Qtr_date.strftime('%B')
            qtr_year = Qtr_date.year
            str_date = str(qtr_mon) + ' ' + str(qtr_year)
            qtr.append(str_date)
            qtr.append(val)

    qtr_df = pd.DataFrame(qtr)
    qtr_df = pd.DataFrame(qtr_df.values.reshape(-1, 2), columns=['qtr', 'Reported_toExchange_Date'])
    qtr_df = (qtr_df.drop_duplicates(subset=['qtr', 'Reported_toExchange_Date']))

    qtr_df = qtr_df[['Reported_toExchange_Date', 'qtr']]
    qtr_df.rename(columns={'Reported_toExchange_Date': 'Exchange_Received'}, inplace=True)
    qtr_df = pd.merge(qtr_df, df, on=['Exchange_Received'], how='left')

    qtrdata = qtr_df['qtr'].unique().tolist()

    df_merge = pd.DataFrame()
    for q in range(0, len(qtrdata)):
        data = qtr_df[qtr_df['qtr'].str.contains(qtrdata[q])]
        group = data.groupby(['name', 'script_code', 'qtr']).size().sort_values(ascending=False).reset_index(
            name='Number_Of_Participat')
        df_merge = pd.concat([df_merge, group], ignore_index=True)

    pivot = pd.pivot_table(df_merge, index=['name', 'script_code'], columns=['qtr'], values=['Number_Of_Participat'],
                           aggfunc=np.sum)

    pivot.columns = pivot.columns.droplevel(0)
    pivot = pivot.reset_index().rename_axis(None, axis=1)

    qtrdata.insert(0, 'script_code')
    qtrdata.insert(0, 'name')
    pivot = pivot[qtrdata]

    AVG = pivot[pivot.columns[4:len(pivot.columns)]]
    pivot['Average'] = AVG.mean(axis=1)
    pivot['Ratio_Avg'] = pivot[pivot.columns[2]] / pivot['Average']

    workbook = xw.Book(REPORT_DIR+'\\'+'intermedent.xlsm')
    ws1 = workbook.sheets['Count_Invester_Meet_Participant']
    ws1.clear()
    ws1.range("A1").options(index=None).value = pivot





def sast():
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=browser_profile())
    driver.get("https://www.bseindia.com/corporates/Sast.html")
    pd.set_option('display.max_columns', None)

    rows = len(driver.find_elements_by_xpath(
        '/html/body/div[5]/div/div/div/div/table/tbody/tr/td/div/table/tbody/tr'))

    cols = len(driver.find_elements_by_xpath(
        '/html/body/div[5]/div/div/div/div/table/tbody/tr/td/div/table/tbody/tr[1]/td'))

    # txtFromDt = driver.find_element_by_xpath('//*[@id="txtFromDt"]')
    # txtFromDt.click()
    # from_date = driver.find_element_by_xpath('//*[@id="ui-datepicker-div"]/table/tbody/tr[3]/td[5]')  # Today_date
    # from_date.click()
    # txtToDt = driver.find_element_by_xpath('//*[@id="txtToDt"]')
    # txtToDt.click()
    # to_date = driver.find_element_by_xpath('//*[@id="ui-datepicker-div"]/table/tbody/tr[3]/td[5]')  # Today_date
    # to_date.click()
    # submit = driver.find_element_by_xpath('//*[@id="btnSubmit"]')
    # submit.click()
    # sleep(3)

    cols = cols + 3

    table_df = []
    # Printing the data of the table
    for r in range(3, rows + 1):  #
        for p in range(1, cols + 1):
            # obtaining the text from each column of the table
            value = driver.find_element_by_xpath(
                "/html/body/div[5]/div/div/div/div/table/tbody/tr/td/div/table/tbody/tr[" + str(
                    r) + "]/td[" + str(p) + "]").text
            print(value)
            table_df.append(value)
            table_df_csv = pd.DataFrame(table_df)
        print()

    table_df_csv = pd.DataFrame(table_df_csv.values.reshape(-1, 14),
                                columns=['Security_Code', 'Security_Name', 'Name_of_Acquirer/Seller',
                                         'Promoter/Promoter_Group',
                                         'Transaction_Period', 'Acq/Sale', 'Mode_of_Buy/Sale',
                                         'Warrants_Transacted_quantity',
                                         'Warrants_Transacted_%', 'HAT_Quantity', 'HAT_%_(w.r.t_Total_Capital)',
                                         'HAT_%_(w.r.t_Diluted_Capital)', 'Regulation', 'Reported_toExchange_Date'])
    driver.close()

    table_df_csv['Transaction_Period'] = table_df_csv['Transaction_Period'].str.split('-')
    df_csv_date = table_df_csv["Transaction_Period"].apply(pd.Series)
    df_csv_date = df_csv_date.rename(columns={0: "dat"})
    df_csv_date['dat'] = df_csv_date['dat'].str.strip()
    df_csv_date = df_csv_date[['dat']]
    # df_csv_date.drop([1], inplace=True, axis=1)
    table_df_csv = pd.concat([table_df_csv, df_csv_date], axis=1)
    table_df_csv['dat'] = pd.to_datetime(table_df_csv['dat'], format='%d/%m/%Y')

    if not os.path.exists(PROCESSED_DIR + '\\Disclosures_under_SEBI_historical.csv'):
        table_df_csv['dat'] = table_df_csv['dat'].dt.strftime('%d-%m-%Y')
        table_df_csv.to_csv(PROCESSED_DIR + '\\Disclosures_under_SEBI_historical.csv', index=False)
    else:
        Historical = pd.read_csv(PROCESSED_DIR + '\\Disclosures_under_SEBI_historical.csv')
        Historical['dat'] = pd.to_datetime(Historical['dat'], format='%d-%m-%Y')
        Historical = pd.concat([table_df_csv, Historical])
        Historical['dat'] = Historical['dat'].dt.strftime('%d-%m-%Y')
        Historical.to_csv(PROCESSED_DIR + '\\Disclosures_under_SEBI_historical.csv', index=False)

    # check and drop duplicates
    dups = pd.read_csv(PROCESSED_DIR + '\\Disclosures_under_SEBI_historical.csv')
    dups = dups.drop_duplicates(
        subset=['Security_Code', 'Security_Name', 'Name_of_Acquirer/Seller', 'Acq/Sale', 'Mode_of_Buy/Sale',
                'Warrants_Transacted_quantity', 'dat'])
    dups.to_csv(PROCESSED_DIR + '\\Disclosures_under_SEBI_historical.csv', index=False)

    dups = dups[dups['Mode_of_Buy/Sale'] == 'Market']
    dups['dat'] = pd.to_datetime(dups['dat'], format='%d-%m-%Y')
    temp_date = date_today - datetime.timedelta(days=4)
    temp_date = pd.to_datetime(temp_date)
    flit_df = dups[dups['dat'] >= temp_date]

    # check Superstars
    flit_df['lower_names'] = flit_df['Name_of_Acquirer/Seller'].str.lower()
    flit_df.dropna(subset=["lower_names"], inplace=True)
    temp_df = superstar_check(flit_df)
    temp_df.drop(['HAT_%_(w.r.t_Diluted_Capital)', 'HAT_%_(w.r.t_Total_Capital)', 'HAT_Quantity', 'lower_names'],
                 inplace=True, axis=1)

    # dump data
    sheet_oi_single = wb.sheets('SAST')
    sheet_oi_single.clear()
    sheet_oi_single.range("A1").options(index=None).value = temp_df
    # get png
    temp_df = temp_df.style.hide_index()
    dfi.export(temp_df, SS_DIR + "\\sast.png")
    wb.save()


def new_sast():
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=browser_profile())
    driver.get("https://www.bseindia.com/corporates/Regulation_29.aspx")
    pd.set_option('display.max_columns', None)

    rows = len(driver.find_elements_by_xpath(
        '/html/body/div[1]/form/div[4]/div/div/div[4]/div/div/div/table/tbody/tr/td/table/tbody/tr[1]/td/table/tbody/tr'))
    cols = len(driver.find_elements_by_xpath(
        '/html/body/div[1]/form/div[4]/div/div/div[4]/div/div/div/table/tbody/tr/td/table/tbody/tr[1]/td/table/tbody/tr[1]/td'))

    table_df = []
    # Printing the data of the table
    for r in range(3, rows + 1):  #
        for p in range(1, cols + 1):
            # obtaining the text from each column of the table
            value = driver.find_element_by_xpath(
                "/html/body/div[1]/form/div[4]/div/div/div[4]/div/div/div/table/tbody/tr/td/table/tbody/tr[1]/td/table/tbody/tr[" + str(
                    r) + "]/td[" + str(p) + "]").text
            print(value)
            table_df.append(value)
            table_df_csv = pd.DataFrame(table_df)
        print()
    print(cols)

    table_df_csv = pd.DataFrame(table_df_csv.values.reshape(-1, 14),
                                columns=['Name_of_the_Target_Company', 'ISIN',
                                         'Names_of_the_promoter/persons_in_promoter_group',
                                         'Number_of_Equity_Shares_held_before_the_acquisition/disposal_No_of_shares',
                                         '%_of_Equity_Shares_held_before_the_acquisition/disposal_%_of_total_equity_share_capital_of_TC',
                                         'Number_of_Equity_Shares_acquired/(disposed)_No_of_shares',
                                         '%_of_Equity_Shares_acquired/(disposed)_%_of_total_equity_share_capital_of_TC',
                                         'Number_of_Equity_Shares_held_after_the_acquisition/disposal_No_of_shares',
                                         '%_of_Equity_Shares_held_after_the_acquisition/disposal_%_of_total_equity_share_Capital_of_TC',
                                         'Date_of_credit_debit', 'Transaction_Type', 'Promoter_Non_Promoter',
                                         'Regulation','Broadcast_Date_And_Time'])
    table_df_csv['Date_of_credit_debit'] = pd.to_datetime(table_df_csv['Date_of_credit_debit'], format='%d %b %Y')
    table_df_csv.drop(['Regulation', 'Broadcast_Date_And_Time'], inplace=True, axis=1)
    driver.close()
    if not os.path.exists(PROCESSED_DIR + '\\Disclosures_system_driven_SAST_historical.csv'):
        table_df_csv['Date_of_credit_debit'] = table_df_csv['Date_of_credit_debit'].dt.strftime('%d-%m-%Y')
        table_df_csv.to_csv(PROCESSED_DIR + '\\Disclosures_system_driven_SAST_historical.csv', index=False)
    else:
        Historical = pd.read_csv(PROCESSED_DIR + '\\Disclosures_system_driven_SAST_historical.csv')
        Historical['Date_of_credit_debit'] = pd.to_datetime(Historical['Date_of_credit_debit'], format='%d-%m-%Y')
        Historical_csv = pd.concat([table_df_csv, Historical])
        Historical_csv['Date_of_credit_debit'] = Historical_csv['Date_of_credit_debit'].dt.strftime('%d-%m-%Y')
        Historical_csv.to_csv(PROCESSED_DIR + '\\Disclosures_system_driven_SAST_historical.csv', index=False)

    # check and drop duplicates
    dups = pd.read_csv(PROCESSED_DIR + '\\Disclosures_system_driven_SAST_historical.csv')
    dups = dups.drop_duplicates(
        subset=['Name_of_the_Target_Company', 'ISIN','Names_of_the_promoter/persons_in_promoter_group',
                'Number_of_Equity_Shares_held_before_the_acquisition/disposal_No_of_shares',
                '%_of_Equity_Shares_held_before_the_acquisition/disposal_%_of_total_equity_share_capital_of_TC',
                'Number_of_Equity_Shares_acquired/(disposed)_No_of_shares',
                '%_of_Equity_Shares_acquired/(disposed)_%_of_total_equity_share_capital_of_TC',
                'Number_of_Equity_Shares_held_after_the_acquisition/disposal_No_of_shares',
                '%_of_Equity_Shares_held_after_the_acquisition/disposal_%_of_total_equity_share_Capital_of_TC',
                'Date_of_credit_debit', 'Transaction_Type', 'Promoter_Non_Promoter'])
    dups.to_csv(PROCESSED_DIR + '\\Disclosures_system_driven_SAST_historical.csv', index=False)

    # merging file for both Sast data
    dups["Promoter_Non_Promoter"].replace({"Promoter": "Yes", "Non-Promoter": "NO"}, inplace=True)
    dups["Transaction_Type"].replace({"Disposal": "SALE", "Acquisition": "ACQ"}, inplace=True)
    stock_metadata = pd.read_csv(METADATA_DIR + '\\StockMetadata.csv')[['ISIN', 'script_code']]
    df_merge = pd.merge(dups, stock_metadata, on=['ISIN'], how='left')

    df_merge = df_merge.rename(
        columns={"script_code": "Security_Code", "Name_of_the_Target_Company": "Security_Name",
                 "Names_of_the_promoter/persons_in_promoter_group": "Name_of_Acquirer/Seller",
                 "Promoter_Non_Promoter": "Promoter/Promoter_Group",
                 "Date_of_credit_debit": "Transaction_Period",
                 "Transaction_Type": "Acq/Sale",
                 "Number_of_Equity_Shares_held_after_the_acquisition/disposal_No_of_shares": "Warrants_Transacted_quantity",
                 "%_of_Equity_Shares_held_after_the_acquisition/disposal_%_of_total_equity_share_Capital_of_TC":
                     "Warrants_Transacted_%"})
    df_merge['Reported_toExchange_Date'] = df_merge['Transaction_Period']
    df_merge['Mode_of_Buy/Sale'] = 'nan'
    df_merge['HAT_Quantity'] = 'nan'
    df_merge['HAT_%_(w.r.t_Total_Capital)'] = 'nan'
    df_merge['HAT_%_(w.r.t_Diluted_Capital)'] = 'nan'
    df_merge['Regulation'] = 'System Driven Disclosure'
    df_merge['dat'] = df_merge['Transaction_Period']
    df_merge = df_merge[["Security_Code", "Security_Name", "Name_of_Acquirer/Seller", "Promoter/Promoter_Group",
                         "Transaction_Period",
                         "Acq/Sale", "Mode_of_Buy/Sale", "Warrants_Transacted_quantity", "Warrants_Transacted_%",
                         "HAT_Quantity",
                         "HAT_%_(w.r.t_Total_Capital)", "HAT_%_(w.r.t_Diluted_Capital)", "Regulation",
                         "Reported_toExchange_Date", "dat"]]
    df_merge['dat'] = pd.to_datetime(df_merge['dat'], format='%d-%m-%Y')
    SEBI_historical = pd.read_csv(PROCESSED_DIR + '\\Disclosures_under_SEBI_historical.csv')
    SEBI_historical['dat'] = pd.to_datetime(SEBI_historical['dat'], format='%d-%m-%Y')
    SEBI_historical = pd.concat([df_merge, SEBI_historical])
    SEBI_historical.to_csv(r'\\192.168.41.190\ashish\\merged_sast.csv')

    qtr = []

    # Historical = pd.read_csv(PROCESSED_DIR + '\\Disclosures_system_driven_SAST_historical.csv')
    # Historical['Date_of_credit_debit'] = pd.to_datetime(Historical['Date_of_credit_debit'], format='%d-%m-%Y')
    # to get quater month
    for val in SEBI_historical['dat']:
        quarter = (val.month - 1) // 3 + 1
        if quarter == 4:
            Qtr_date = datetime.datetime(val.year, 12, 31)
            qtr_mon = Qtr_date.strftime('%B')
            qtr_year = Qtr_date.year
            str_date = str(qtr_mon) + ' ' + str(qtr_year)
            qtr.append(str_date)
            qtr.append(val)
        elif quarter == 1:
            Qtr_date = datetime.datetime(val.year, 3, 31)
            qtr_mon = Qtr_date.strftime('%B')
            qtr_year = Qtr_date.year
            str_date = str(qtr_mon) + ' ' + str(qtr_year)
            qtr.append(str_date)
            qtr.append(val)
        elif quarter == 2:
            Qtr_date = datetime.datetime(val.year, 6, 30)
            qtr_mon = Qtr_date.strftime('%B')
            qtr_year = Qtr_date.year
            str_date = str(qtr_mon) + ' ' + str(qtr_year)
            qtr.append(str_date)
            qtr.append(val)
        else:
            Qtr_date = datetime.datetime(val.year, 9, 30)
            qtr_mon = Qtr_date.strftime('%B')
            qtr_year = Qtr_date.year
            str_date = str(qtr_mon) + ' ' + str(qtr_year)
            qtr.append(str_date)
            qtr.append(val)

    qtr_df = pd.DataFrame(qtr)
    qtr_df = pd.DataFrame(qtr_df.values.reshape(-1, 2), columns=['qtr', 'Reported_toExchange_Date'])
    qtr_df = (qtr_df.drop_duplicates(subset=['qtr', 'Reported_toExchange_Date']))

    df_final = pd.merge(SEBI_historical, qtr_df, how='left', left_on='dat', right_on='Reported_toExchange_Date')

    # today data
    today = dt.now()
    # today = datetime.datetime(2021, 9, 21)
    df_today = df_final[df_final['dat'] >= today - relativedelta(days=1)]
    df_today.drop(['Name_of_Acquirer/Seller', 'Mode_of_Buy/Sale', 'HAT_Quantity',
                   'HAT_%_(w.r.t_Total_Capital)', 'HAT_%_(w.r.t_Diluted_Capital)',
                   'Reported_toExchange_Date_x', 'Reported_toExchange_Date_y'], inplace=True, axis=1)

    # filters and group
    df_final = df_final[(df_final['Promoter/Promoter_Group'] == 'Yes') |
                        (df_final['Promoter/Promoter_Group'] == '`Yes') |
                        (df_final['Promoter/Promoter_Group'] == 'Ys')]

    df_final = df_final[(df_final['Acq/Sale'] == 'ACQ') | (df_final['Acq/Sale'] == 'SALE')]
    df_final = df_final[(df_final['Mode_of_Buy/Sale'] == 'Market') |
                        (df_final['Mode_of_Buy/Sale'] == 'Market Sale') |
                        (df_final['Mode_of_Buy/Sale'] == 'Market Purchase')]
    df_final['Warrants_Transacted_quantity'] = df_final['Warrants_Transacted_quantity'].astype(float).fillna(0)
    df_final['Warrants_Transacted_%'] = df_final['Warrants_Transacted_%'].astype(float).fillna(0)
    df_final = df_final.groupby(['Security_Code', 'Acq/Sale', 'qtr']).agg(
        {'Warrants_Transacted_quantity': ['sum', 'count'], 'Warrants_Transacted_%': ['sum']}).reset_index()
    df_final.columns = ['Security_Code', 'Acq/Sale', 'qtr', 'Warrants_Transacted_quantity_sum',
                        'count', 'Warrants_Transacted_%_sum']

    StockMetadata = pd.read_csv(METADATA_DIR + '\\StockMetadata.csv')[
        ['MarketCap', 'companyName', 'script_code', 'ISIN']]
    df_final = pd.merge(df_final, StockMetadata, how='left', left_on='Security_Code', right_on='script_code')
    df_final['Warrants_Transacted_%_sum'] = df_final['Warrants_Transacted_%_sum'].astype(float)
    df_final.sort_values(by=['Warrants_Transacted_%_sum'], ascending=False,
                         inplace=True)  # desc
    df_final = df_final[df_final['Warrants_Transacted_quantity_sum'] != 0]

    Disposal = df_final[df_final['Acq/Sale'] == 'SALE']
    Disposal = Disposal[~Disposal.companyName.isnull()]
    Disposal.drop(['script_code', 'ISIN'], inplace=True, axis=1)

    Acquisition = df_final[df_final['Acq/Sale'] == 'ACQ']
    Acquisition = Acquisition[~Acquisition.companyName.isnull()]
    Acquisition.drop(['script_code', 'ISIN'], inplace=True, axis=1)
    net_df = pd.merge(Acquisition, Disposal, on=['Security_Code', 'qtr'], how='outer')

    net_df['net_share'] = net_df['Warrants_Transacted_quantity_sum_x'].fillna(0) - net_df[
        'Warrants_Transacted_quantity_sum_y'].fillna(0)
    net_df['net_share_perct'] = net_df['Warrants_Transacted_%_sum_x'].fillna(0) - net_df[
        'Warrants_Transacted_%_sum_y'].fillna(0)

    Acquisition = net_df[net_df['net_share'] > 0]
    Acquisition.drop(['Warrants_Transacted_quantity_sum_x', 'Warrants_Transacted_%_sum_x', 'Acq/Sale_y',
                      'Warrants_Transacted_quantity_sum_y', 'Warrants_Transacted_%_sum_y', 'MarketCap_y',
                      'companyName_y', 'count_y'], inplace=True, axis=1)
    Acquisition['qtr'] = pd.to_datetime(Acquisition['qtr'], format='%B %Y')
    Acquisition.sort_values(by=['qtr', 'net_share_perct'], ascending=False, inplace=True)
    Acquisition['qtr'] = Acquisition['qtr'].dt.strftime('%b-%y')

    Disposal = net_df[net_df['net_share'] < 0]
    Disposal.drop(['Warrants_Transacted_quantity_sum_x', 'Warrants_Transacted_%_sum_x', 'Acq/Sale_x',
                   'Warrants_Transacted_quantity_sum_y', 'Warrants_Transacted_%_sum_y', 'MarketCap_x',
                   'companyName_x', 'count_x'], inplace=True, axis=1)
    Disposal['qtr'] = pd.to_datetime(Disposal['qtr'], format='%B %Y')
    Disposal['net_share'] = abs(Disposal['net_share'])
    Disposal['net_share_perct'] = abs(Disposal['net_share_perct'])
    Disposal.sort_values(by=['qtr', 'net_share_perct'], ascending=False, inplace=True)
    Disposal['qtr'] = Disposal['qtr'].dt.strftime('%b-%y')

    # dump data to excel
    sheet_oi_single = wb.sheets('Disposal')
    sheet_oi_single.clear()
    sheet_oi_single.range("A1").options(index=None).value = Disposal
    Disposal = Disposal.head(37)
    format_dict = {'Security_Code': '{:.0f}', 'count_y': '{:.0f}', 'net_share': '{:.0f}', 'net_share_perct': '{:.2f}'}
    Disposal = (Disposal.style.hide_index().format(format_dict)
                   .bar(color='#FFA07A', vmin=100_000, subset=['net_share_perct'], align='zero'))
    dfi.export(Disposal, SS_DIR + "\\Disposal.png")

    sheet_oi_single = wb.sheets('Acquisition')
    sheet_oi_single.clear()
    sheet_oi_single.range("A1").options(index=None).value = Acquisition
    Acquisition = Acquisition.head(37)
    format_dict = {'Security_Code': '{:.0f}', 'count_x': '{:.0f}', 'net_share': '{:.0f}', 'net_share_perct': '{:.2f}'}
    Acquisition = (Acquisition.style.hide_index().format(format_dict)
                   .bar(color='#FFA07A', vmin=100_000, subset=['net_share_perct'], align='zero'))
    dfi.export(Acquisition, SS_DIR + "\\Acquisition.png")

    sheet_oi_single = wb.sheets('today_data_sast')
    sheet_oi_single.clear()
    sheet_oi_single.range("A1").options(index=None).value = df_today
    df_today = df_today.round({"Warrants_Transacted_quantity": 2})
    df_today = df_today.style.hide_index()
    dfi.export(df_today, SS_DIR + "\\today_data_sast.png")

    wb.save()


def insider_trading():
    insider_csv = pd.read_csv(r'\\192.168.41.190\program\stockdata\processed\insider_trading_sast.csv', encoding='cp1252')
    insider_csv['Reported to Exchange'] = pd.to_datetime(insider_csv['Reported to Exchange'], format='%d-%m-%Y')
    qtr = []

    # to get quater month
    for val in insider_csv['Reported to Exchange']:
        quarter = (val.month - 1) // 3 + 1
        if quarter == 4:
            Qtr_date = datetime.datetime(val.year, 12, 31)
            qtr_mon = Qtr_date.strftime('%B')
            qtr_year = Qtr_date.year
            str_date = str(qtr_mon) + ' ' + str(qtr_year)
            qtr.append(str_date)
            qtr.append(val)
        elif quarter == 1:
            Qtr_date = datetime.datetime(val.year, 3, 31)
            qtr_mon = Qtr_date.strftime('%B')
            qtr_year = Qtr_date.year
            str_date = str(qtr_mon) + ' ' + str(qtr_year)
            qtr.append(str_date)
            qtr.append(val)
        elif quarter == 2:
            Qtr_date = datetime.datetime(val.year, 6, 30)
            qtr_mon = Qtr_date.strftime('%B')
            qtr_year = Qtr_date.year
            str_date = str(qtr_mon) + ' ' + str(qtr_year)
            qtr.append(str_date)
            qtr.append(val)
        else:
            Qtr_date = datetime.datetime(val.year, 9, 30)
            qtr_mon = Qtr_date.strftime('%B')
            qtr_year = Qtr_date.year
            str_date = str(qtr_mon) + ' ' + str(qtr_year)
            qtr.append(str_date)
            qtr.append(val)

    qtr_df = pd.DataFrame(qtr)
    qtr_df = pd.DataFrame(qtr_df.values.reshape(-1, 2), columns=['qtr', 'Reported_toExchange_Date'])
    qtr_df = (qtr_df.drop_duplicates(subset=['qtr', 'Reported_toExchange_Date']))

    df_final = pd.merge(insider_csv, qtr_df, how='left', left_on='Reported to Exchange', right_on='Reported_toExchange_Date')

    # today data
    today = dt.now()
    # today = datetime.datetime(2021, 9, 21)
    df_today = df_final[df_final['Reported to Exchange'] >= today - relativedelta(days=1)]

    # filters and group
    df_final = df_final[(df_final['Category of person'] == 'Promoter Group') |
                        (df_final['Category of person'] == 'Promoter & Director') |
                        (df_final['Category of person'] == 'Promoter')|
                        (df_final['Category of person'] == 'Promoters Immediate Relative')|
                        (df_final['Category of person'] == 'Promoter and Director')]

    df_final = df_final[(df_final['Transaction Type'] == 'Acquisition') | (df_final['Transaction Type'] == 'Disposal')]
    df_final = df_final[(df_final['Mode of  Acquisition'] == 'Market') |
                        (df_final['Mode of  Acquisition'] == 'Market Sale') |
                        (df_final['Mode of  Acquisition'] == 'Market Purchase')]

    df_final['Number'] = df_final['Number'].astype(float)
    df_final = df_final.groupby(['Security Code', 'Security Name', 'Transaction Type', 'qtr']).agg(
        {'Number': ['sum', 'count']}).reset_index()
    df_final.columns = ['Security_Code', 'Security_Name', 'Transaction Type', 'qtr', 'Number_sum',
                        'count']

    df_final['Number_sum'] = df_final['Number_sum'].astype(float)
    df_final.sort_values(by=['qtr'], ascending=False,
                         inplace=True)  # desc
    
    df_final = df_final[df_final['Number_sum'] != 0]

    Disposal = df_final[df_final['Transaction Type'] == 'Disposal']

    Acquisition = df_final[df_final['Transaction Type'] == 'Acquisition']
    net_df = pd.merge(Acquisition, Disposal, on=['Security_Code', 'qtr'], how='outer')

    net_df['net_share'] = net_df['Number_sum_x'].fillna(0) - net_df[
        'Number_sum_y'].fillna(0)

    sum_base = pd.read_csv(r'\\192.168.41.190\program\stockdata\processed\sum_base_shareholdings.csv')[['Scrip_code',
                                                                                                        'Total_no._shares_held_promoter',
                                                                                                        'Total_no._shares_held_all_sum']]

    Acquisition = net_df[net_df['net_share'] > 0]
    Acquisition.drop(['Security_Name_y', 'Transaction Type_y', 'Number_sum_y', 'count_y', 'Number_sum_x'], inplace=True, axis=1)
    # Acquisition['qtr'] = pd.to_datetime(Acquisition['qtr'], format='%B %Y')
    # Acquisition['qtr'] = Acquisition['qtr'].dt.strftime('%b-%y')
    Acquisition = pd.merge(Acquisition, sum_base, how='left', left_on='Security_Code', right_on='Scrip_code')
    Acquisition['shares_promotor_holdings'] = ((Acquisition['Total_no._shares_held_promoter'] / Acquisition['Total_no._shares_held_all_sum']) * 100).round(2)
    Acquisition['net_chng_perct']= ((Acquisition['net_share'] / Acquisition['Total_no._shares_held_all_sum'] )* 100).round(2)
    Acquisition.drop(['Total_no._shares_held_promoter', 'Total_no._shares_held_all_sum', 'Scrip_code'], inplace=True, axis=1)
    Acquisition.sort_values(by=['net_chng_perct'], ascending=False,
                         inplace=True)

    Disposal = net_df[net_df['net_share'] < 0]
    Disposal.drop(['Security_Name_x', 'Transaction Type_x', 'Number_sum_x', 'count_x', 'Number_sum_y'], inplace=True, axis=1)
    # Disposal['qtr'] = pd.to_datetime(Disposal['qtr'], format='%B %Y')
    Disposal['net_share'] = abs(Disposal['net_share'])
    # Disposal['qtr'] = Disposal['qtr'].dt.strftime('%b-%y')
    Disposal = pd.merge(Disposal, sum_base, how='left', left_on='Security_Code', right_on='Scrip_code')
    Disposal['shares_promotor_holdings'] = ((Disposal['Total_no._shares_held_promoter'] / Disposal[
        'Total_no._shares_held_all_sum']) * 100).round(2)
    Disposal['net_chng_perct'] = ((Disposal['net_share'] / Disposal['Total_no._shares_held_all_sum']) * 100).round(2)
    Disposal.drop(['Total_no._shares_held_promoter', 'Total_no._shares_held_all_sum', 'Scrip_code'], inplace=True, axis=1)
    Disposal.sort_values(by=['net_chng_perct'], ascending=False,
                            inplace=True)

    # dump data to excel
    sheet_oi_single = wb.sheets('insider_dis')
    sheet_oi_single.clear()
    sheet_oi_single.range("A1").options(index=None).value = Disposal

    sheet_oi_single = wb.sheets('insider_acq')
    sheet_oi_single.clear()
    sheet_oi_single.range("A1").options(index=None).value = Acquisition
    wb.save()


def bulk_nse():
    dls = "https://archives.nseindia.com/content/equities/bulk.csv"
    urllib.request.urlretrieve(dls, RAW_DIR + "\\bulk.csv")

    bulk_csv = pd.read_csv(RAW_DIR + '\\bulk.csv')
    bulk_csv['Date'] = pd.to_datetime(bulk_csv['Date'], format='%d-%b-%Y')

    if not os.path.exists(PROCESSED_DIR + '\\bulk_historical.csv'):
        bulk_csv['Date'] = bulk_csv['Date'].dt.strftime('%d-%m-%Y')
        bulk_csv.to_csv(PROCESSED_DIR + '\\bulk_historical.csv', index=False)
    else:
        Historical = pd.read_csv(PROCESSED_DIR + '\\bulk_historical.csv')
        Historical['Date'] = pd.to_datetime(Historical['Date'], format='%d-%m-%Y')
        if bulk_csv['Date'].max() != Historical['Date'].max():
            Historical = pd.concat([bulk_csv, Historical])
            Historical['Date'] = Historical['Date'].dt.strftime('%d-%m-%Y')
            Historical.to_csv(PROCESSED_DIR + '\\bulk_historical.csv', index=False)


def bulk_bse():
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=browser_profile())
    driver.get("https://www.bseindia.com/markets/equity/eqreports/bulk_deals.aspx")
    pd.set_option('display.max_columns', None)

    rows = len(driver.find_elements_by_xpath(
        '/html/body/form/div[4]/div/div/div[3]/div/div/table/tbody/tr/td/div/table/tbody/tr'))

    cols = len(driver.find_elements_by_xpath(
        '/html/body/form/div[4]/div/div/div[3]/div/div/table/tbody/tr/td/div/table/tbody/tr[2]/td'))

    table_df = []

    for r in range(2, rows + 1):  #
        for p in range(1, cols + 1):
            # obtaining the text from each column of the table
            value = driver.find_element_by_xpath(
                '/html/body/form/div[4]/div/div/div[3]/div/div/table/tbody/tr/td/div/table/tbody/tr[' + str(
                    r) + ']/td[' + str(p) + ']').text
            print(value)
            table_df.append(value)
            table_df_csv = pd.DataFrame(table_df)
        print()

    table_df_csv = pd.DataFrame(table_df_csv.values.reshape(-1, 7),
                                columns=['Date', 'Symbol', 'Security Name', 'Client Name',
                                         'Buy/Sell', 'Quantity Traded', 'Trade Price / Wght. Avg. Price'])
    table_df_csv['Date'] = pd.to_datetime(table_df_csv['Date'], format='%d/%m/%Y')
    driver.close()

    if not os.path.exists(PROCESSED_DIR + '\\bulk_bse_historical.csv'):
        table_df_csv['Date'] = table_df_csv['Date'].dt.strftime('%d-%m-%Y')
        table_df_csv.to_csv(PROCESSED_DIR + '\\bulk_bse_historical.csv', index=False)
    else:
        Historical = pd.read_csv(PROCESSED_DIR + '\\bulk_bse_historical.csv')
        Historical['Date'] = pd.to_datetime(Historical['Date'], format='%d-%m-%Y')
        if table_df_csv['Date'].max() != Historical['Date'].max():
            Historical = pd.concat([table_df_csv, Historical])
            Historical['Date'] = Historical['Date'].dt.strftime('%d-%m-%Y')
            Historical.to_csv(PROCESSED_DIR + '\\bulk_bse_historical.csv', index=False)

    # return table_df_csv
    table_df_csv['Date'] = pd.to_datetime(table_df_csv['Date'], format='%d-%m-%Y')

    # concate NSE_BULK_FILE, BSE_BULK_FILE
    bulk_historical = pd.read_csv(PROCESSED_DIR + '\\bulk_historical.csv')
    bulk_historical['Date'] = pd.to_datetime(bulk_historical['Date'], format='%d-%m-%Y')
    max_date = np.datetime64(bulk_historical['Date'].max())
    df_bulk_max = bulk_historical[bulk_historical['Date'] == max_date]
    Stock_metadata_bse = pd.read_csv(METADATA_DIR + '\\StockMetadata.csv')[['issued_shares', 'script_code']]
    Stock_metadata_nse = pd.read_csv(METADATA_DIR + '\\StockMetadata.csv')[['Symbol', 'issued_shares']]
    table_df_csv['Symbol'] = table_df_csv['Symbol'].astype(float)
    df_merge_bse = pd.merge(table_df_csv, Stock_metadata_bse, how='left', left_on='Symbol', right_on='script_code')
    df_merge_nse = pd.merge(df_bulk_max, Stock_metadata_nse, how='left', left_on='Symbol', right_on='Symbol')
    final_df = pd.concat([df_merge_bse, df_merge_nse])
    final_df['Quantity Traded'] = final_df['Quantity Traded'].str.replace(',', '').astype(float)
    final_df = final_df[final_df.issued_shares != '-']
    final_df['issued_shares'] = final_df['issued_shares'].astype(float)
    final_df['perct_chng'] = ((final_df['Quantity Traded'] / final_df['issued_shares']) * 100).round(2)
    final_df['Date'] = final_df['Date'].dt.strftime('%d-%m-%Y')
    # stock_metadata_df = pd.read_csv(METADATA_DIR + '\\StockMetadata.csv')
    # final_df['equity_perct'] = (final_df['equity_perct'] / final_df['equity_perct']) * 100

    # check Superstars
    final_df['lower_names'] = final_df['Client Name'].str.lower()
    superstar_df = superstar_check(final_df)

    # get PNG
    date_today = datetime.date.today()  # - datetime.timedelta(days=1)
    superstar_df['date_today'] = date_today

    sheet_oi_single = wb.sheets('superstar_bulk_data')
    sheet_oi_single.range("A1").options(index=None).value = superstar_df
    wb.save()
    dfi.export(superstar_df, SS_DIR + "\\superstar_bulk_data.png")


def get_link():
    quater = pd.read_csv(METADATA_DIR + '\\quaters.csv')
    quater['date'] = pd.to_datetime(quater['date'], format="%d-%m-%Y")
    quater["date_1"] = quater["date"].dt.date
    df = quater[quater['date_1'] < TODAY_DATE]
    date_max = df['date'].max()
    df_max_date = quater[quater['date'] == date_max]
    df_max_date = pd.DataFrame(df_max_date)
    # qtr_id = df_max_date['qtr_id'][2]
    index_val = int(df_max_date.index.values)
    qtr_id = df_max_date['qtr_id'][index_val]

    driver = webdriver.Chrome(ChromeDriverManager().install(), options=browser_profile())
    time.sleep(get_random_wait(initial_limit=2, upper_limit=5))
    driver.get("https://www.bseindia.com/corporates/Sharehold_Searchnew.aspx")
    url_main = "https://www.bseindia.com/corporates/Sharehold_Searchnew.aspx"
    r = requests.get(url_main, headers=header)

    dfs = pd.read_html(r.text)
    scriptcode_int_df = pd.read_csv(
        RAW_DIR + "\\Today_intermedent_Script_" + str(dt.today().date()) + ".csv")

    # scriptcode_int_df = pd.read_csv("Today_intermedent_Script_2021-08-10.csv")
    scrips = scriptcode_int_df['Security Name']
    codes = scriptcode_int_df['Security Code']

    qtr_link = pd.DataFrame()
    for scrip_name, scrip_code in zip(scrips, codes):
        time.sleep(get_random_wait(initial_limit=2, upper_limit=5))
        driver.get("https://www.bseindia.com/corporates/Sharehold_Searchnew.aspx")

        input_security = driver.find_element_by_xpath('//input[@class="textbox2"]')
        input_security.click()
        input_security.send_keys(scrip_code)
        time.sleep(get_random_wait(initial_limit=2, upper_limit=5))
        select_security = driver.find_element_by_xpath('//div[@id="ajax_response_smart"]/ul/li')
        time.sleep(get_random_wait(initial_limit=2, upper_limit=5))
        select_security.click()
        period = driver.find_element_by_xpath('//option[text()="Today"]')  # Last 1 Week
        period.click()
        submit = driver.find_element_by_xpath('//input[@value="Submit"]')
        submit.click()
        time.sleep(get_random_wait(initial_limit=2, upper_limit=5))
        base_url = driver.current_url
        driver.execute_script("window.scrollTo(0, 500);")
        time.sleep(get_random_wait(initial_limit=2, upper_limit=5))

        dataframe_base = pd.DataFrame()
        dataframe_prmo = pd.DataFrame()
        dataframe_public = pd.DataFrame()

        for i in range(1, 7):
            i = i + 1
            try:
                quarter_link = driver.find_element_by_xpath(
                    '//table[@class="mGrid"]/tbody/tr[{}]/td[4]/a'.format(i)).get_attribute('href')
                time.sleep(get_random_wait(initial_limit=2, upper_limit=5))
                qtrid = quarter_link[76:-9]

                if (qtrid == format(qtr_id, '.2f')):
                    print(qtrid)
                else:
                    scrip_name = pd.Series(scrip_name)
                    qtrid = pd.Series(qtrid)
                    scrip_name = pd.concat([scrip_name, qtrid], axis=1)
                    qtr_link = pd.concat([qtr_link, scrip_name])
            except:
                continue
    qtr_link = qtr_link.rename(columns={0: "Security Name", 1: "qtr_link"})
    scriptcode_int_df = pd.merge(scriptcode_int_df, qtr_link, on=['Security Name'])
    scriptcode_int_df.to_csv(RAW_DIR + "\\Today_intermedent_Script_" + str(dt.today().date()) + ".csv",
                             index=None)
    # scriptcode_int_df.to_csv("Today_intermedent_Script_2021-08-10.csv", index=None)
    sheet_oi_single = wb.sheets('intermedent_data')
    sheet_oi_single.clear()
    sheet_oi_single.range("A2").options(index=None).value = scriptcode_int_df

    # get png
    scriptcode_int_df = scriptcode_int_df.style.hide_index()
    dfi.export(scriptcode_int_df, SS_DIR + "\\intermedent_data.png")
    driver.close()
    wb.save()
    app = xw.apps.active
    app.quit()


def scrapping_jobs(scrip_code, scrip_name, dataframe_base, dataframe_prmo, dataframe_public, qtr_id, link_quater):
    print(scrip_name, scrip_code)

    """
    Shareholding pattern scrapping
    """
    r = requests.get("https://www.bseindia.com/corporates/shpSecurities.aspx?scripcd=" + str(scrip_code)
                     + "&qtrid=" + str(qtr_id) + "&Flag=New", headers=header)
    dfs = pd.read_html(r.text)
    # dataframe_base
    temp_df = dfs[4]
    temp_df.columns = temp_df.iloc[0]
    temp_df = temp_df.iloc[3:-1]
    temp_df = temp_df[['Category of shareholder', 'No. of shareholders', 'Total no. shares held',
                       'Shareholding as a % of total no. of shares (calculated as per SCRR, 1957)As a % of (A+B+C2)']]

    temp_df.insert(loc=0, column='Date', value=[TODAY_DATE] * temp_df.shape[0])
    temp_df.insert(loc=1, column='Scrip_code', value=[scrip_code] * temp_df.shape[0])
    temp_df.insert(loc=2, column='Company', value=[scrip_name] * temp_df.shape[0])
    temp_df.insert(loc=3, column='Quarter', value=[str(dfs[2][3][1]).split(':')[-1]] * temp_df.shape[0])

    dataframe_base = pd.concat([dataframe_base, temp_df])

    """
    Promoter shareholding pattern scrapping
    """
    promoter_url = 'https://www.bseindia.com/corporates/shpPromoterNGroup.aspx?scripcd=' + str \
        (scrip_code) + '&qtrid=' + str(qtr_id) + '&QtrName=' + str(link_quater)

    promoter_r = requests.get(promoter_url, headers=header)
    promoter_df = pd.read_html(promoter_r.text)
    temp_promoter_df = promoter_df[3]
    temp_promoter_df.columns = temp_promoter_df.iloc[0]
    temp_promoter_df = temp_promoter_df.iloc[3:-2]
    nationality_list, category_list = get_nationality_category_promoter(temp_promoter_df)
    temp_promoter_df.insert(loc=0, column='Date', value=[TODAY_DATE] * temp_promoter_df.shape[0])
    temp_promoter_df.insert(loc=1, column='Scrip_code', value=[scrip_code] * temp_promoter_df.shape[0])
    temp_promoter_df.insert(loc=2, column='Company', value=[scrip_name] * temp_promoter_df.shape[0])
    temp_promoter_df.insert(loc=3, column='Quarter',
                            value=[str(dfs[2][3][1]).split(':')[-1]] * temp_promoter_df.shape[0])
    temp_promoter_df.insert(loc=4, column='Nationality', value=nationality_list)
    temp_promoter_df.insert(loc=5, column='Category', value=category_list)
    final_df = temp_promoter_df[['Date', 'Scrip_code', 'Company', 'Quarter', 'Nationality', 'Category',
                                 'Category of shareholder', 'Nos. of shareholders', 'Total nos. shares held',
                                 'Shareholding as a % of total no. of shares (calculated as per SCRR, 1957)As a % of (A+B+C2)']]
    try:
        delete_row = final_df[final_df['Category of shareholder'] == 'Sub Total A1'].index
        final_df = final_df.drop(delete_row)
    except:
        pass

    try:
        delete_row_na = final_df[final_df['Total nos. shares held'] is None].index
        final_df = final_df.drop(delete_row_na)
    except:
        pass

    dataframe_prmo = pd.concat([dataframe_prmo, final_df])

    """
     Public shareholding pattern scrapping
     """
    public_url = 'https://www.bseindia.com/corporates/shpPublicShareholder.aspx?scripcd=' + str(
        scrip_code) + '&qtrid=' + str(qtr_id) + '&QtrName=' + str(link_quater)

    public_r = requests.get(public_url, headers=header)
    public_df = pd.read_html(public_r.text)
    temp_public_df = public_df[3]
    temp_public_df.columns = temp_public_df.iloc[0]
    temp_public_df = temp_public_df.iloc[6:-5]
    institution_list, category_list = get_nationality_category_public(temp_public_df)
    temp_public_df.insert(loc=0, column='Date', value=[TODAY_DATE] * temp_public_df.shape[0])
    temp_public_df.insert(loc=1, column='Scrip_code', value=[scrip_code] * temp_public_df.shape[0])
    temp_public_df.insert(loc=2, column='Company', value=[scrip_name] * temp_public_df.shape[0])
    temp_public_df.insert(loc=3, column='Quarter',
                          value=[str(dfs[2][3][1]).split(':')[-1]] * temp_public_df.shape[0])
    temp_public_df.insert(loc=4, column='Nationality', value=institution_list)
    temp_public_df.insert(loc=5, column='Category', value=category_list)
    final_df = temp_public_df[['Date', 'Scrip_code', 'Company', 'Quarter', 'Nationality', 'Category',
                               'Category & Name of the Shareholders', 'No. of shareholder',
                               'Total no. shares held',
                               'Shareholding % calculated as per SCRR, 1957 As a % of (A+B+C2)']]
    try:
        delete_row_1 = final_df[final_df['Category & Name of the Shareholders'] == 'Sub Total B1'].index
        final_df = final_df.drop(delete_row_1)
    except:
        pass

    try:
        delete_row_2 = final_df[final_df['Category & Name of the Shareholders'] == 'Sub Total B2'].index
        final_df = final_df.drop(delete_row_2)
    except:
        pass

    try:
        delete_row_3 = final_df[final_df['Category & Name of the Shareholders'] == 'Sub Total B3'].index
        final_df = final_df.drop(delete_row_3)
    except:
        pass

    final_df.rename(columns={'Category & Name of the Shareholders': 'Category of shareholder',
                             'No. of shareholder': 'Nos. of shareholders',
                             'Total no. shares held': 'Total nos. shares held',
                             'Shareholding % calculated as per SCRR, 1957 As a % of (A+B+C2)':
                                 'Shareholding as a % of total no. of shares (calculated as per SCRR, 1957)As a % of (A+B+C2)'
                             },
                    inplace=True)

    # final_df.dropna()
    try:
        delete_row_na = final_df[final_df['Total nos. shares held'] is None].index
        final_df = final_df.drop(delete_row_na)
    except:
        pass

    dataframe_public = pd.concat([dataframe_public, final_df])

    return dataframe_base, dataframe_prmo, dataframe_public


def call_scraping_job():
    quater = pd.read_csv(METADATA_DIR + '\\quaters.csv')
    quater['date'] = pd.to_datetime(quater['date'], format="%d-%m-%Y")
    quater["date_1"] = quater["date"].dt.date
    df = quater[quater['date_1'] < TODAY_DATE]
    date_max = df['date'].max()
    df_max_date = quater[quater['date'] == date_max]
    df_max_date = pd.DataFrame(df_max_date)
    index_val = int(df_max_date.index.values)
    link_quater = df_max_date['link_quater'][index_val]

    try:
        dataframe = pd.read_csv(RAW_DIR + "\\Today_intermedent_Script_" + str(dt.today().date()) + ".csv")
    except Exception as e:
        e = e + ' Intermetent file not  found.'
        pass
    # dataframe = pd.read_csv("Today_intermedent_Script_2021-08-02.csv")

    dataframe_base = pd.DataFrame()
    dataframe_prmo = pd.DataFrame()
    dataframe_public = pd.DataFrame()

    for ind in dataframe.index:
        try:
            dataframe_base, dataframe_prmo, dataframe_public = scrapping_jobs(dataframe['Security Code'][ind],
                                                                              dataframe['Security Name'][ind],
                                                                              dataframe_base, dataframe_prmo,
                                                                              dataframe_public,
                                                                              dataframe['qtr_link'][ind], link_quater)
        except:
            continue

    frame = pd.concat([dataframe_prmo, dataframe_public])
    frame.drop(['Quarter'], inplace=True, axis=1)

    intermedent_scriptcode_df = pd.read_csv(
        RAW_DIR + "\\Today_intermedent_Script_" + str(dt.today().date()) + ".csv")
    intermedent_scriptcode_df['For Quarter Ending'] = pd.to_datetime(intermedent_scriptcode_df['For Quarter Ending'],
                                                                     format="%d-%m-%Y")
    intermedent_scriptcode_df = intermedent_scriptcode_df.rename(columns={"Security Code": "Scrip_code"})
    intermedent_scriptcode_df.drop(['Security Name', 'Industry', 'For Quarter Ending', 'XBRL'], inplace=True, axis=1)

    frame = frame.reset_index()
    frame = pd.merge(frame, intermedent_scriptcode_df, on=['Scrip_code'])

    frame = frame[
        ["index", "Date", "Scrip_code", "Company", "Quarter", "Nationality", "Category", "Category of shareholder",
         "Nos. of shareholders", "Total nos. shares held",
         "Shareholding as a % of total no. of shares (calculated as per SCRR, 1957)As a % of (A+B+C2)"]]
    frame.to_csv(METADATA_DIR + '\\final.csv', index=False)

    current_quarter = frame['Quarter'].unique()
    current_scriptcode = frame['Scrip_code'].unique()
    current_quarter = pd.to_datetime(current_quarter, format='%b-%y')

    updated_final = pd.read_csv(PROCESSED_DIR + '\\Historical_detail_ShareHolding_Pattern.csv')
    updated_final['Quarter'] = pd.to_datetime(updated_final['Quarter'], format='%b-%y')
    updated_final['Date'] = pd.to_datetime(updated_final['Date'], format='%d-%m-%Y')

    updated_final.drop(updated_final[(updated_final['Quarter'].isin(current_quarter)) & (
        updated_final['Scrip_code'].isin(current_scriptcode))].index, inplace=True)

    frame = add_details_in_headers_with_noentries()
    frame['Quarter'] = pd.to_datetime(frame['Quarter'], format='%b-%y')
    frame['Date'] = TODAY_DATE
    frame['Date'] = pd.to_datetime(frame['Date'], format='%Y-%m-%d')

    Historical_details_df = pd.concat([updated_final, frame])
    Historical_details_df['Quarter'] = pd.to_datetime(Historical_details_df['Quarter'], format='%b-%y')
    Historical_details_df['Date'] = pd.to_datetime(Historical_details_df['Date'], format='%d-%m-%Y')
    Historical_details_df['Quarter'] = Historical_details_df['Quarter'].dt.strftime('%b-%y')
    Historical_details_df['Date'] = Historical_details_df['Date'].dt.strftime('%d-%m-%Y')
    Historical_details_df.to_csv(PROCESSED_DIR + '\\Historical_detail_ShareHolding_Pattern.csv', index=None)

    # read old base_shareholding_pattern.csv, from today data df(dataframe_base) get script code, today script and quarter code ko delete from old file
    current_quarter = dataframe_base['Quarter'].unique()
    current_scriptcode = dataframe_base['Scrip_code'].unique()
    current_quarter = pd.to_datetime(current_quarter, format='%d %b %Y')

    hist_base_shareholding_df = pd.read_csv(PROCESSED_DIR + '\\Historical_base_shareholding_pattern.csv')
    hist_base_shareholding_df['Quarter'] = pd.to_datetime(hist_base_shareholding_df['Quarter'], format='%b-%y')
    hist_base_shareholding_df['Date'] = pd.to_datetime(hist_base_shareholding_df['Date'], format='%d-%m-%Y')
    hist_base_shareholding_df.drop(
        hist_base_shareholding_df[(hist_base_shareholding_df['Quarter'].isin(current_quarter))
                                  & (hist_base_shareholding_df['Scrip_code'].isin(current_scriptcode))].index,
        inplace=True)

    dataframe_base['Date'] = pd.to_datetime(dataframe_base['Date'], format="%Y-%m-%d")

    df_final = pd.merge(dataframe_base, intermedent_scriptcode_df, on=['Scrip_code'])
    df_final.drop(['Quarter_x', 'qtr_link'], inplace=True, axis=1)
    df_final = df_final.rename(columns={"Quarter_y": "Quarter"})
    df_final['Quarter'] = pd.to_datetime(df_final['Quarter'], format="%b-%y")
    df_final = pd.concat([df_final, hist_base_shareholding_df])
    df_final = df_final[["Date", "Scrip_code", "Company", "Quarter", "Category of shareholder", "No. of shareholders",
                         "Total no. shares held",
                         "Shareholding as a % of total no. of shares (calculated as per SCRR, 1957)As a % of (A+B+C2)"]]
    df_final['Date'] = df_final['Date'].dt.strftime('%d-%m-%Y')
    df_final['Quarter'] = df_final['Quarter'].dt.strftime('%b-%y')
    df_final.to_csv(PROCESSED_DIR + '\\Historical_base_shareholding_pattern.csv', index=None)


def sendmail():
    EMAIL_ADDRESS = 'researchequity10@gmail.com'
    EMAIL_PASSWORD = 'noxtajxjdmzdjlkb'

    contacts = ['researchequity10@gmail.com', 'engineers1030164@gmail.com', 'vijitramavat@gmail.com']
    msg = EmailMessage()
    msg['Subject'] = ' Shareholding Pattern as of ' + str(TODAY_DATE)
    msg['From'] = EMAIL_ADDRESS
    msg['To'] = ['rohnkoria@gmail.com', 'engineers1030164@gmail.com', 'vijitramavat@gmail.com']

    # now create a Content-ID for the image
    image_cid = make_msgid()
    image1_cid = make_msgid()
    image2_cid = make_msgid()
    image3_cid = make_msgid()
    image4_cid = make_msgid()
    image5_cid = make_msgid()
    image6_cid = make_msgid()
    image7_cid = make_msgid()
    image8_cid = make_msgid()
    image9_cid = make_msgid()
    image10_cid = make_msgid()
    image11_cid = make_msgid()
    image12_cid = make_msgid()
    image13_cid = make_msgid()
    image14_cid = make_msgid()
    image15_cid = make_msgid()
    image16_cid = make_msgid()
    image17_cid = make_msgid()
    image18_cid = make_msgid()
    image19_cid = make_msgid()
    image20_cid = make_msgid()

    msg.add_alternative("""\
    <!DOCTYPE html>
    <html>
        <body>
            <h1 style="color:SlateGray;">intermittent Scripts</h1>
            <img src="cid:{image_cid}">
            <h1 style="color:SlateGray;">Mrg Trading Sum</h1>
            <img src="cid:{image1_cid}">
            <h1 style="color:SlateGray;">Mrg Trading Amt</h1>
            <img src="cid:{image2_cid}">
            <h1 style="color:SlateGray;">SAST</h1>
            <img src="cid:{image3_cid}">
            <h1 style="color:SlateGray;">Bulk Data</h1>
            <img src="cid:{image4_cid}">
            <h1 style="color:SlateGray;">superstar intermittent</h1>
            <img src="cid:{image5_cid}">
            <h1 style="color:SlateGray;">FII intermittent</h1>
            <img src="cid:{image6_cid}">
            <h1 style="color:SlateGray;">superstar quaterly</h1>
            <img src="cid:{image10_cid}">
            <h1 style="color:SlateGray;">FII quaterly</h1>
            <img src="cid:{image11_cid}">
            <h1 style="color:SlateGray;">Acquisition Sast</h1>
            <img src="cid:{image7_cid}">
            <h1 style="color:SlateGray;">Disposal Sast</h1>
            <img src="cid:{image8_cid}">
            <h1 style="color:SlateGray;">today_data Sast</h1>
            <img src="cid:{image9_cid}">
            <h1 style="color:SlateGray;">asm tab 1</h1>
            <img src="cid:{image12_cid}">
            <h1 style="color:SlateGray;">asm tab 2</h1>
            <img src="cid:{image13_cid}">
            <h1 style="color:SlateGray;">ibc tab 1</h1>
            <img src="cid:{image14_cid}">
            <h1 style="color:SlateGray;">ibc tab 2</h1>
            <img src="cid:{image20_cid}">
            <h1 style="color:SlateGray;">st_asm Annexure I</h1>
            <img src="cid:{image15_cid}">
            <h1 style="color:SlateGray;">st_asm Annexure II</h1>
            <img src="cid:{image16_cid}">
            <h1 style="color:SlateGray;">Consolidated - ST ASM</h1>
            <img src="cid:{image17_cid}">
            <h1 style="color:SlateGray;">stocks_in_ASM</h1>
            <img src="cid:{image18_cid}">
            <h1 style="color:SlateGray;">stocks_cameout_of_ASM</h1>
            <img src="cid:{image19_cid}">
        </body>
    </html>
    """.format(image_cid=image_cid[1:-1], image1_cid=image1_cid[1:-1], image2_cid=image2_cid[1:-1],
               image3_cid=image3_cid[1:-1], image4_cid=image4_cid[1:-1], image5_cid=image5_cid[1:-1],
               image6_cid=image6_cid[1:-1]
               , image10_cid=image10_cid[1:-1], image9_cid=image9_cid[1:-1], image8_cid=image8_cid[1:-1],
               image7_cid=image7_cid[1:-1], image11_cid=image11_cid[1:-1], image12_cid=image12_cid[1:-1]
               , image13_cid=image13_cid[1:-1], image14_cid=image14_cid[1:-1]
               , image15_cid=image15_cid[1:-1], image16_cid=image16_cid[1:-1]
               , image17_cid=image17_cid[1:-1], image18_cid=image18_cid[1:-1]
               , image19_cid=image19_cid[1:-1], image20_cid=image20_cid[1:-1]), subtype='html')

    with open(SS_DIR + '//intermedent_data.png', 'rb') as img:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image_cid)
    with open(SS_DIR + '//mrg_trading.png', 'rb') as img1:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img1.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img1.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image1_cid)
    with open(SS_DIR + '//mrg_trading_amt.png', 'rb') as img2:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img2.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img2.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image2_cid)
    with open(SS_DIR + '//sast.png', 'rb') as img3:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img3.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img3.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image3_cid)
    with open(SS_DIR + '//superstar_bulk_data.png', 'rb') as img4:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img4.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img4.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image4_cid)
    with open(SS_DIR + '//Detail_tab_superstar.png', 'rb') as img5:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img5.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img5.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image5_cid)
    with open(SS_DIR + '//Header_tab_fii.png', 'rb') as img6:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img6.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img6.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image6_cid)
    with open(SS_DIR + '//Acquisition.png', 'rb') as img7:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img7.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img7.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image7_cid)
    with open(SS_DIR + '//Disposal.png', 'rb') as img8:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img8.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img8.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image8_cid)
    with open(SS_DIR + '//today_data_sast.png', 'rb') as img9:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img9.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img9.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image9_cid)
    with open(SS_DIR + '//quaterly_Detail_tab_superstar.png', 'rb') as img10:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img10.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img10.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image10_cid)
    with open(SS_DIR + '//qtr_Header_tab_fii.png', 'rb') as img11:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img11.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img11.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image11_cid)
    with open(SS_DIR + '//asm_df_tab1.png', 'rb') as img12:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img12.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img12.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image12_cid)
    with open(SS_DIR + '//asm_df_tab2.png', 'rb') as img13:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img13.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img13.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image13_cid)
    with open(SS_DIR + '//ibc_tab1.png', 'rb') as img14:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img14.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img14.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image14_cid)
    with open(SS_DIR + '//ibc_tab2.png', 'rb') as img20:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img20.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img20.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image20_cid)
    with open(SS_DIR + '//st_asm_df_tab1.png', 'rb') as img15:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img15.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img15.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image15_cid)
    with open(SS_DIR + '//st_asm_df_tab2.png', 'rb') as img16:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img16.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img16.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image16_cid)
    with open(SS_DIR + '//st_asm_df_tab3.png', 'rb') as img17:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img17.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img17.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image17_cid)
    with open(SS_DIR + '//stocks_in_ASM_1.png', 'rb') as img18:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img18.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img18.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image18_cid)
    with open(SS_DIR + '//stocks_cameout_ASM.png', 'rb') as img19:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img19.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img19.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image19_cid)
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        smtp.send_message(msg)


if __name__ == '__main__':
    # get png
    empty_df = pd.DataFrame()
    dfi.export(empty_df, SS_DIR + "\\intermedent_data.png")
    dfi.export(empty_df, SS_DIR + "\\Header_tab_fii.png")
    dfi.export(empty_df, SS_DIR + "\\Detail_tab_superstar.png")
    sheet_oi_single = wb.sheets('Fund_Raising')
    sheet_oi_single.clear()
    fund_raise = pd.read_csv(RAW_DIR + '\\fund_raise.csv')
    sheet_oi_single.range("A2").options(index=None).value = fund_raise
    wb.save()

    try:
        week_high()
        ic('week_high')
        count_announcement()
        ic('count_announcement')
        investor_meet_by_participant()
        ic('investor_meet_by_participant')

    except Exception as e:
        print(e)
        pass


    try:
        bulk_nse()
        ic('bulk_nse')
        bulk_bse()
        ic('bulk_bse')
    except Exception as e:
        print(e)
        pass
    try:
        sast()
        ic('sast')
        new_sast()
        ic('new_sast')
        insider_trading()
        ic('insider_trading')
    except Exception as e:
        print(e)
        pass

    try:
        from surveillance_investigation import *
        surveillance_investigation()
        ic('surveillance_investigation')
        from nilesh_code import *
        ic('nilesh code')
        execution_status('pass', file, logging.ERROR, TODAY_DATE, 1)
    except Exception as e:
        print(e)
        execution_status(str(e), file, logging.ERROR, TODAY_DATE, 0)
        sendmail_err(file, e)
    try:
        # get_link()
        ic('get_link')
        # call_scraping_job()
        ic('call_scraping_job')
    except Exception as e:
        print(e)
        pass

    TODAY_DATE = TODAY_DATE.strftime('%d-%m-%Y')
    File = 'intermittent'

    # call for superstar
    try:
        FileType = 'Superstar'
        bse_shareholdings(FileType, TODAY_DATE, File)
        print('Superstar')
    except Exception as e:
        print(e)
    # call for FII
    try:
        FileType = 'FII'
        bse_shareholdings(FileType, TODAY_DATE, File)
    except Exception as e:
        print(e)

    sendmail()
    print('sendmail')
