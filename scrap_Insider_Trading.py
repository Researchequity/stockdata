from utils import *
from icecream import ic

file = os.path.basename(__file__)


def insider_trading():
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=browser_profile())
    driver.get("https://www.bseindia.com/corporates/Insider_Trading_new.aspx")
    pd.set_option('display.max_columns', None)
    rows = len(driver.find_elements_by_xpath(
        '/html/body/div[1]/form/div[4]/div[1]/div/div[3]/div[2]/div/div/div/table[1]/tbody/tr/td/div/table/tbody/tr'))
    cols = len(driver.find_elements_by_xpath(
        '/html/body/div[1]/form/div[4]/div[1]/div/div[3]/div[2]/div/div/div/table[1]/tbody/tr/td/div/table/tbody/tr[1]/td'))
    ic(rows)
    ic(cols)
    table_df = []
    # Printing the data of the table
    for r in range(3, rows + 1):  #
        for p in range(1, cols + 6):
            # obtaining the text from each column of the table
            value = driver.find_element_by_xpath(
                '/html/body/div[1]/form/div[4]/div[1]/div/div[3]/div[2]/div/div/div/table[1]/tbody/tr/td/div/table/tbody/tr[' + str(
                    r) + ']/td[' + str(p) + ']').text
            print(value)
            table_df.append(value)
            results_calendar_csv = pd.DataFrame(table_df)
        print()
    results_calendar_csv = pd.DataFrame(results_calendar_csv.values.reshape(-1, 16),
                                        columns=['Security Code', 'Security Name', 'Name of Person',
                                                 'Category of person',
                                                 'Securities_held_pre_Transaction_temp', 'Type of Securities', 'Number',
                                                 'Value', 'Transaction Type', 'Securities_held_post_Transaction_temp',
                                                 'Period', 'Mode of  Acquisition', 'Type of Contract',
                                                 'Buy Value (Units~)', 'Sale Value (Units~)', 'Reported to Exchange'])
    results_calendar_csv['split_column'] = results_calendar_csv['Securities_held_pre_Transaction_temp'].str.split('(')
    results_calendar_csv[['Securities_held_pre_Transaction', '%_of_Securities_held_pre']] = pd.DataFrame \
        (results_calendar_csv['split_column'].tolist(), index=results_calendar_csv.index)
    results_calendar_csv.drop(['Securities_held_pre_Transaction_temp', 'split_column'], inplace=True, axis=1)
    results_calendar_csv['split_col'] = results_calendar_csv['Securities_held_post_Transaction_temp'].str.split('(')
    results_calendar_csv[['Securities_held_post_Transaction', '%_of_Securities_held_post']] = pd.DataFrame \
        (results_calendar_csv['split_col'].tolist(), index=results_calendar_csv.index)
    results_calendar_csv.drop(['Securities_held_post_Transaction_temp', 'split_col'], inplace=True, axis=1)
    results_calendar_csv['date_split'] = results_calendar_csv['Period'].str.split(' ')
    results_calendar_csv[['From date', 'To date']] = pd.DataFrame \
        (results_calendar_csv['date_split'].tolist(), index=results_calendar_csv.index)
    results_calendar_csv.drop(['date_split', 'Period', 'Type of Contract', 'Buy Value (Units~)', 'Sale Value (Units~)'],
                              inplace=True, axis=1)
    results_calendar_csv['%_of_Securities_held_pre'] = results_calendar_csv['%_of_Securities_held_pre']. \
        str.replace(')', '', regex=True)
    results_calendar_csv['%_of_Securities_held_post'] = results_calendar_csv['%_of_Securities_held_post']. \
        str.replace(')', '', regex=True)
    results_calendar_csv = results_calendar_csv[["Security Code", "Security Name", "Name of Person",
                                                 "Category of person", "Type of Securities",
                                                 "Securities_held_pre_Transaction", "%_of_Securities_held_pre",
                                                 "Number", "Value", "Transaction Type",
                                                 "Securities_held_post_Transaction", "%_of_Securities_held_post",
                                                 "From date", "To date", "Mode of  Acquisition",
                                                 "Reported to Exchange"]]
    results_calendar_csv['Reported to Exchange'] = pd.to_datetime(results_calendar_csv['Reported to Exchange'],
                                                                  format='%d/%m/%Y')
    if os.path.exists(PROCESSED_DIR + '\\insider_trading_sast.csv'):
        insider_trade_sast = pd.read_csv(PROCESSED_DIR + '\\insider_trading_sast.csv', encoding="latin-1")
        insider_trade_sast['Reported to Exchange'] = pd.to_datetime(insider_trade_sast['Reported to Exchange'],
                                                                    format='%d-%m-%Y')
        if results_calendar_csv['Reported to Exchange'].max() != insider_trade_sast['Reported to Exchange'].max():
            Historical = pd.concat([results_calendar_csv, insider_trade_sast])
            Historical['Reported to Exchange'] = Historical['Reported to Exchange'].dt.strftime('%d-%m-%Y')
            Historical.to_csv(PROCESSED_DIR + '\\insider_trading_sast.csv', index=False)
    else:
        results_calendar_csv['Reported to Exchange'] = results_calendar_csv['Reported to Exchange'].dt.strftime(
            '%d-%m-%Y')
        results_calendar_csv.to_csv(PROCESSED_DIR + '\\insider_trading_sast.csv', index=False)
    driver.close()


def cleaning_df():
    insider_trade_sast = pd.read_csv(PROCESSED_DIR + '\\insider_trading_sast.csv', encoding='cp1252')
    insider_trade_sast['Reported to Exchange'] = pd.to_datetime(insider_trade_sast['Reported to Exchange'],
                                                                format='%d-%m-%Y')
    insider_trade_sast = insider_trade_sast.rename(columns={"Security Code": "Security_Code"
        , "Security Name": "Security_Name", "Name of Person": "Name_of_Acquirer/Seller"
        , "Category of person": "Promoter/Promoter_Group"
        , "Transaction Type": "Acq/Sale", "Mode of  Acquisition": "Mode_of_Buy/Sale",
                                                            "Reported to Exchange": "Reported_toExchange_Date"})
    insider_trade_sast = insider_trade_sast[insider_trade_sast['%_of_Securities_held_pre'] != '-']
    insider_trade_sast = insider_trade_sast[insider_trade_sast['%_of_Securities_held_pre'].notna()]
    insider_trade_sast = insider_trade_sast[insider_trade_sast['%_of_Securities_held_post'] != '-']
    insider_trade_sast = insider_trade_sast[insider_trade_sast['%_of_Securities_held_post'].notna()]
    insider_trade_sast = insider_trade_sast[insider_trade_sast['Number'] != '-']
    insider_trade_sast = insider_trade_sast[insider_trade_sast['Number'].notna()]
    insider_trade_sast['%_of_Securities_held_pre'] = insider_trade_sast['%_of_Securities_held_pre'].astype(float)

    insider_trade_sast['chng'] = (insider_trade_sast['Securities_held_pre_Transaction'] /
                                  insider_trade_sast['%_of_Securities_held_pre']) * 100
    insider_trade_sast['Number'] = insider_trade_sast['Number'].astype(str).str.replace(',', '').astype(float)
    insider_trade_sast['final_chng'] = (insider_trade_sast['Number'] / insider_trade_sast['chng']) * 100
    insider_trade_sast = insider_trade_sast[insider_trade_sast['Mode_of_Buy/Sale'].notna()]
    insider_trade_sast = insider_trade_sast.loc[
        insider_trade_sast['Mode_of_Buy/Sale'].str.contains("pledge", case=False)]
    insider_trade_sast["Mode_of_Buy/Sale"].replace({"Creation Of Pledge": "Pledge Creation",
                                                    "Invocation Of Pledged": "Pledge Creation",
                                                    "Invocation of pledge": "Pledge Creation",
                                                    "Invocation Of Pledge & Market": "Pledge Creation",
                                                    "Pledge": "Pledge Creation",
                                                    "Pledged Invocation": "Pledge Creation",
                                                    "Revocation Of Pledge": "Pledge Released"}, inplace=True)
    # insider_trade_sast = insider_trade_sast[(insider_trade_sast['Mode_of_Buy/Sale'] == 'Pledge Creation') |
    #                                         (insider_trade_sast['Mode_of_Buy/Sale'] == 'Pledge Released')]
    insider_trade_sast["Acq/Sale"].replace({"Disposal": "SALE", "Acquisition": "ACQ"}, inplace=True)
    qtr = []

    # to get quater month
    for val in insider_trade_sast['Reported_toExchange_Date']:
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
    df_pledge = pd.merge(insider_trade_sast, qtr_df, how='left', left_on='Reported_toExchange_Date',
                         right_on='Reported_toExchange_Date')

    df_pledge['Number'] = np.where(df_pledge['Mode_of_Buy/Sale'] == 'Pledge Released', (df_pledge['Number'] * -1),
                                   df_pledge['Number'])
    df_pledge['final_chng'] = np.where(df_pledge['Mode_of_Buy/Sale'] == 'Pledge Released',
                                       (df_pledge['final_chng'] * -1), df_pledge['final_chng'])

    df_pledge = df_pledge.groupby(['Security_Code', 'Mode_of_Buy/Sale', 'qtr']).agg(
        {'Number': ['sum'], 'final_chng': ['sum']}).reset_index()
    df_pledge.columns = ['Security_Code', 'Mode_of_Buy/Sale', 'qtr', 'Number_sum', 'perct_sum']
    StockMeta = pd.read_csv(METADATA_DIR + '\\StockMetadata.csv')[
        ['companyName', 'script_code']]
    pledge = pd.merge(df_pledge, StockMeta, how='left', left_on='Security_Code', right_on='script_code')
    pledge['qtr'] = pd.to_datetime(pledge['qtr'], format='%B %Y')
    pledge.sort_values(by=['qtr', 'Number_sum'], ascending=False, inplace=True)
    pledge['qtr'] = pledge['qtr'].dt.strftime('%b-%y')
    pledge.drop(['script_code'], inplace=True, axis=1)
    pledge = pledge.round({"perct_sum": 2})
    pledge.to_csv(r'\\192.168.41.190\ashish\pledge.csv', index=None)


if __name__ == '__main__':

    try:
        insider_trading()
        print('insider_trading')
        cleaning_df()
        execution_status('pass', file, logging.ERROR, '', 1)
    except Exception as e:
        print(e)
        execution_status(str(e), file, logging.ERROR, '', 0)
        sendmail_err(file, e)

