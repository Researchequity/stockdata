import pandas as pd
import requests
from datetime import datetime as datetimedatime
from dateutil.relativedelta import relativedelta
from bse_shareholdings import *
from numpy import *
from utils import *
file = os.path.basename(__file__)

TODAY_DATE = datetime.date.today() #+ datetime.timedelta(days=60)


def get_scriptcode_scraping():
    quater = pd.read_csv(METADATA_DIR + '\\quaters.csv')
    quater['date'] = pd.to_datetime(quater['date'], format="%d-%m-%Y")
    quater["date_1"] = quater["date"].dt.date
    df = quater[quater['date_1'] < TODAY_DATE]
    date_max = df['date'].max()
    df_max_date = quater[quater['date'] == date_max]
    df_max_date = pd.DataFrame(df_max_date)
    index_val = int(df_max_date.index.values)
    qtr_id = df_max_date['qtr_id'][index_val]
    link_quater = df_max_date['link_quater'][index_val]


    r = requests.get("https://www.bseindia.com/corporates/Sharehold_Searchnew.aspx", headers=header)

    driver = webdriver.Chrome(ChromeDriverManager().install(), options=browser_profile())
    driver.get("https://www.bseindia.com/corporates/Sharehold_Searchnew.aspx")

    period = driver.find_element_by_xpath('//option[text()="Today"]')  # Last 1 Week
    period.click()
    submit = driver.find_element_by_xpath('//input[@value="Submit"]')
    submit.click()

    dfs = pd.read_html(r.text)
    pd.set_option('display.max_columns', None)
    dataframe = dfs[4]
    print(dataframe['Security Name'])

    i = 1
    pagenumber = 2
    while i < 13:
        i = i + 1
        print(i)
        try:

            page_link = driver.find_element_by_xpath(
                '//table[@class="mGrid"]/tbody/tr[@class="pgr"]/td/table/tbody/tr/td[{}]/a'.format(i))
            page_number = driver.find_element_by_xpath(
                '//table[@class="mGrid"]/tbody/tr[@class="pgr"]/td/table/tbody/tr/td[{}]'.format(i))
            print("page_number:", page_number.text)
            print("page_var", pagenumber)
            if page_number.text == "...":
                i = 1
            elif int(page_number.text) < pagenumber:
                print("page_number:", page_number.text)
                continue

            page_link.click()

            pagenumber = pagenumber + 1
            table = driver.find_element_by_id('ContentPlaceHolder1_gvData')

            import csv

            with open(METADATA_DIR + '\\script_multi_page.csv', 'w', newline='') as csvfile:
                wr = csv.writer(csvfile)
                for row in table.find_elements_by_css_selector('tr'):
                    wr.writerow([col.text for col in row.find_elements_by_css_selector('th')])
                    if row.text[0] == '5':
                        wr.writerow([col.text for col in row.find_elements_by_css_selector('td')])

            df_page = pd.read_csv(METADATA_DIR + '\\script_multi_page.csv')

            dataframe = pd.concat([dataframe, df_page])
            dataframe = dataframe[dataframe['XBRL'].isnull()]

            print(dataframe['Security Name'])

        except:
            continue

    #to get previous quater month
    today = datetimedatime.now()
    # today = datetime.datetime(2022, 1, 1) #for test days month
    quarter = (today.month - 1) // 3 + 1
    ic(quarter)
    if quarter == 1:
        Qtr_date = datetime.datetime((today.year - 1), 12, 31)
        qtr_mon = Qtr_date.strftime('%B')
        qtr_year = Qtr_date.year
        str_date = str(qtr_mon) + ' ' + str(qtr_year)
        ic(str_date)
    elif quarter == 2:
        Qtr_date = datetime.datetime(today.year, 3, 31)
        qtr_mon = Qtr_date.strftime('%B')
        qtr_year = Qtr_date.year
        str_date = str(qtr_mon) + ' ' + str(qtr_year)
        ic(str_date)
    elif quarter == 3:
        Qtr_date = datetime.datetime(today.year, 6, 30)
        qtr_mon = Qtr_date.strftime('%B')
        qtr_year = Qtr_date.year
        str_date = str(qtr_mon) + ' ' + str(qtr_year)
        ic(str_date)
    else:
        Qtr_date = datetime.datetime(today.year, 9, 30)
        qtr_mon = Qtr_date.strftime('%B')
        qtr_year = Qtr_date.year
        str_date = str(qtr_mon) + ' ' + str(qtr_year)
        ic(str_date)
    scriptcode_qtr_end_df = dataframe[dataframe['For Quarter Ending'] == str_date]
    scriptcode_qtr_end_df.to_csv(RAW_DIR + "\\Today_Script_" + str(datetimedatime.today().date()) + ".csv", index=None)

    #create list for intermitent quarter data
    scriptcode_int_df = dataframe[~dataframe['For Quarter Ending'].isin(qtr_list)]
    ic(scriptcode_int_df)

    # calculating quater end
    scriptcode_int_df['For Quarter Ending'] = pd.to_datetime(scriptcode_int_df['For Quarter Ending'], format="%d %b %Y")
    df_qtr = pd.DataFrame()
    for i in scriptcode_int_df['For Quarter Ending']:
        qtr_month = i.strftime('%b')
        qtr_year = i.strftime('%y')
        if (qtr_month == 'Mar' or qtr_month == 'Jun' or qtr_month == 'Sep' or qtr_month == 'Dec'):
            qtr_month = pd.to_datetime(qtr_month, format="%b")
            next_month = qtr_month - relativedelta(months=1)
            next_month = next_month.strftime('%b')
            qtr_str = next_month + '-' + qtr_year
            qtr_str = pd.Series(qtr_str)
            i = pd.Series(i)
            qtr_str = pd.concat([qtr_str, i], axis=1)
            df_qtr = pd.concat([df_qtr, qtr_str])
        elif (qtr_month == 'Feb' or qtr_month == 'May' or qtr_month == 'Aug' or qtr_month == 'Nov'):
            qtr_str = qtr_month + '-' + qtr_year
            qtr_str = pd.Series(qtr_str)
            i = pd.Series(i)
            qtr_str = pd.concat([qtr_str, i], axis=1)
            df_qtr = pd.concat([df_qtr, qtr_str])
        else:
            qtr_month = pd.to_datetime(qtr_month, format="%b")
            next_month = qtr_month + relativedelta(months=1)
            next_month = next_month.strftime('%b')
            qtr_str = next_month + '-' + qtr_year
            qtr_str = pd.Series(qtr_str)
            i = pd.Series(i)
            qtr_str = pd.concat([qtr_str, i], axis=1)
            df_qtr = pd.concat([df_qtr, qtr_str])
    df_qtr = df_qtr.rename(columns={1: "For Quarter Ending", 0: "quater_i"})
    if df_qtr.empty:
        print('No Intermedent Data!')
    else:

        scriptcode_int_df = pd.merge(scriptcode_int_df, df_qtr, on=['For Quarter Ending'])
        scriptcode_int_df = scriptcode_int_df.rename(columns={"quater_i": "Quarter"})
        scriptcode_int_df['For Quarter Ending'] = scriptcode_int_df['For Quarter Ending'].dt.strftime('%d-%m-%Y')
        scriptcode_int_df = (scriptcode_int_df.drop_duplicates(subset=['Security Code', 'For Quarter Ending']))
        scriptcode_int_df.to_csv(RAW_DIR + "\\Today_intermedent_Script_" + str(datetimedatime.today().date()) + ".csv", index=None)

    driver.close()

    return qtr_id,link_quater


def scrapping_jobs(scrip_code, scrip_name, dataframe_base, dataframe_prmo, dataframe_public, qtr_id, link_quater):
    print(scrip_name, scrip_code)
    new_quater = pd.read_csv(METADATA_DIR + '//quaters.csv')[['qtr_id', 'quarter']]
    new_quater = new_quater[new_quater['qtr_id'] == qtr_id]
    new_quater = pd.DataFrame(new_quater)
    index_val = int(new_quater.index.values)
    new_quater = new_quater['quarter'][index_val]
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
    # temp_df.insert(loc=3, column='Quarter', value=[str(dfs[2][3][1]).split(':')[-1]] * temp_df.shape[0])
    temp_df['Quarter'] = new_quater

    dataframe_base = pd.concat([dataframe_base, temp_df])

    """
    Promoter shareholding pattern scrapping
    """
    # promoter_url = 'https://www.bseindia.com/corporates/shpPromoterNGroup.aspx?scripcd=' + str(
    #     scrip_code) + '&qtrid=110.00&QtrName=June%202021'
    promoter_url = 'https://www.bseindia.com/corporates/shpPromoterNGroup.aspx?scripcd='+ str\
        (scrip_code) +'&qtrid='+str(qtr_id)+'&QtrName='+str(link_quater)

    promoter_r = requests.get(promoter_url, headers=header)
    promoter_df = pd.read_html(promoter_r.text)
    temp_promoter_df = promoter_df[3]
    temp_promoter_df.columns = temp_promoter_df.iloc[0]
    temp_promoter_df = temp_promoter_df.iloc[3:-2]
    nationality_list, category_list = get_nationality_category_promoter(temp_promoter_df)
    temp_promoter_df.insert(loc=0, column='Date', value=[TODAY_DATE] * temp_promoter_df.shape[0])
    temp_promoter_df.insert(loc=1, column='Scrip_code', value=[scrip_code] * temp_promoter_df.shape[0])
    temp_promoter_df.insert(loc=2, column='Company', value=[scrip_name] * temp_promoter_df.shape[0])
    # temp_promoter_df.insert(loc=3, column='Quarter',
    #                         value=[str(dfs[2][3][1]).split(':')[-1]] * temp_promoter_df.shape[0])
    temp_promoter_df['Quarter'] = new_quater
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
    # public_url = 'https://www.bseindia.com/corporates/shpPublicShareholder.aspx?scripcd=' + str(
    #     scrip_code) + '&qtrid=110.00&QtrName=June%202021'
    public_url = 'https://www.bseindia.com/corporates/shpPublicShareholder.aspx?scripcd=' + str(
        scrip_code) + '&qtrid='+str(qtr_id)+'&QtrName='+str(link_quater)

    public_r = requests.get(public_url, headers=header)
    public_df = pd.read_html(public_r.text)
    temp_public_df = public_df[3]
    temp_public_df.columns = temp_public_df.iloc[0]
    temp_public_df = temp_public_df.iloc[6:-5]
    institution_list, category_list = get_nationality_category_public(temp_public_df)
    temp_public_df.insert(loc=0, column='Date', value=[TODAY_DATE] * temp_public_df.shape[0])
    temp_public_df.insert(loc=1, column='Scrip_code', value=[scrip_code] * temp_public_df.shape[0])
    temp_public_df.insert(loc=2, column='Company', value=[scrip_name] * temp_public_df.shape[0])
    # temp_public_df.insert(loc=3, column='Quarter',
    #                       value=[str(dfs[2][3][1]).split(':')[-1]] * temp_public_df.shape[0])
    temp_public_df['Quarter'] = new_quater
    temp_public_df.insert(loc=4, column='Nationality', value=institution_list)
    temp_public_df.insert(loc=5, column='Category', value=category_list)
    final_df = temp_public_df[['Date', 'Scrip_code', 'Company', 'Quarter', 'Nationality', 'Category',
                               'Category & Name of the Shareholders', 'No. of shareholder',
                               'Total no. shares held', 'Shareholding % calculated as per SCRR, 1957 As a % of (A+B+C2)']]
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
                             'Shareholding as a % of total no. of shares (calculated as per SCRR, 1957)As a % of (A+B+C2)'},
                    inplace=True)

    # final_df.dropna()
    try:
        delete_row_na = final_df[final_df['Total nos. shares held'] is None].index
        final_df = final_df.drop(delete_row_na)
    except:
        pass

    dataframe_public = pd.concat([dataframe_public, final_df])


    return dataframe_base, dataframe_prmo, dataframe_public


def call_scraping_job(qtr_id,link_quater):

    dataframe = pd.read_csv(RAW_DIR + "\\Today_Script_" + str(datetimedatime.today().date()) + ".csv")
    # dataframe = dataframe.head(10)
    dataframe_base = pd.DataFrame()
    dataframe_prmo = pd.DataFrame()
    dataframe_public = pd.DataFrame()

    # for scrip_name, scrip_code in zip(scrips, codes):
    for ind in dataframe.index:
        try:
            dataframe_base, dataframe_prmo, dataframe_public = scrapping_jobs(dataframe['Security Code'][ind],
                                                                              dataframe['Security Name'][ind],
                                                                              dataframe_base, dataframe_prmo,
                                                                              dataframe_public, qtr_id, link_quater)
        except:
            continue

    frame = pd.concat([dataframe_prmo, dataframe_public])

    # because of util.py has function add_details_in_headers_with_noentries, which need to reset index
    frame = frame.reset_index()


    # read old updated_final.csv, from today data df (frame) get script code, today script code and quarter ko delete from old file
    current_quarter = frame['Quarter'].unique()
    current_quarter = pd.to_datetime(current_quarter, format='%b-%y')
    current_scriptcode = frame['Scrip_code'].unique()

    updated_final = pd.read_csv(PROCESSED_DIR + '\\Historical_detail_ShareHolding_Pattern.csv') #path +
    updated_final['Quarter'] = pd.to_datetime(updated_final['Quarter'], format='%b-%y')
    updated_final['Date'] = pd.to_datetime(updated_final['Date'], format='%d-%m-%Y')
    updated_final.drop(updated_final[(updated_final['Quarter'].isin(current_quarter)) & (updated_final['Scrip_code'].isin(current_scriptcode))].index, inplace=True)

    frame.to_csv(METADATA_DIR + '\\final.csv')
    frame = add_details_in_headers_with_noentries()
    frame['Quarter'] = pd.to_datetime(frame['Quarter'], format='%b-%y')
    frame['Date'] = TODAY_DATE
    frame['Date'] = pd.to_datetime(frame['Date'], format='%Y-%m-%d')

    Historical_details_df = pd.concat([updated_final, frame])
    Historical_details_df['Quarter'] = pd.to_datetime(Historical_details_df['Quarter'], format='%b-%y')
    Historical_details_df['Date'] = pd.to_datetime(Historical_details_df['Date'], format='%d-%m-%Y')
    Historical_details_df_csv = Historical_details_df
    Historical_details_df_csv['Quarter'] = Historical_details_df_csv['Quarter'].dt.strftime('%b-%y')
    Historical_details_df_csv['Date'] = Historical_details_df_csv['Date'].dt.strftime('%d-%m-%Y')
    Historical_details_df.to_csv(PROCESSED_DIR + '\\Historical_detail_ShareHolding_Pattern.csv', index=None)

    Historical_details_df['Quarter'] = pd.to_datetime(Historical_details_df['Quarter'], format="%b-%y")
    latest_details_shareholding_df = Historical_details_df.groupby(['Scrip_code']).agg(
        {'Quarter': [np.max]}).reset_index()
    latest_details_shareholding_df.columns = ['Scrip_code', 'Quarter']
    latest_details_shareholding_df = pd.merge(latest_details_shareholding_df, Historical_details_df, on=['Scrip_code', 'Quarter'],
                        how='left')
    latest_details_shareholding_df['Quarter'] = pd.to_datetime(latest_details_shareholding_df['Quarter'], format='%b-%y')
    latest_details_shareholding_df['Date'] = pd.to_datetime(latest_details_shareholding_df['Date'], format='%d-%m-%Y')
    latest_details_shareholding_df['Quarter'] = latest_details_shareholding_df['Quarter'].dt.strftime('%b-%y')
    latest_details_shareholding_df['Date'] = latest_details_shareholding_df['Date'].dt.strftime('%d-%m-%Y')
    latest_details_shareholding_df = (latest_details_shareholding_df.drop_duplicates(subset=['Scrip_code', 'Quarter', 'Company', 'Nationality',
                                                                                             'Category of shareholder',
                                                         'Nos. of shareholders', 'Header', 'Total nos. shares held',
                                                         'Shareholding as a % of total no. of shares (calculated as per SCRR, 1957)As a % of (A+B+C2)']))

    latest_details_shareholding_df.to_csv(PROCESSED_DIR + '\\latest_detail_shareholdings.csv', index=None)


    # read old base_shareholding_pattern.csv, from today data df(dataframe_base) get script code, today script and quarter code ko delete from old file
    current_quarter = dataframe_base['Quarter'].unique()
    current_quarter = pd.to_datetime(current_quarter, format='%b-%y')
    current_scriptcode = dataframe_base['Scrip_code'].unique()
    hist_base_shareholding_df = pd.read_csv(PROCESSED_DIR + '\\Historical_base_shareholding_pattern.csv') #path +
    hist_base_shareholding_df['Quarter'] = pd.to_datetime(hist_base_shareholding_df['Quarter'], format='%b-%y')
    hist_base_shareholding_df['Date'] = pd.to_datetime(hist_base_shareholding_df['Date'], format='%d-%m-%Y')
    hist_base_shareholding_df.drop(hist_base_shareholding_df[(hist_base_shareholding_df['Quarter'].isin(current_quarter) ) & (hist_base_shareholding_df['Scrip_code'].isin(current_scriptcode))].index, inplace=True)

    dataframe_base['Quarter'] = pd.to_datetime(dataframe_base['Quarter'],format="%b-%y")
    dataframe_base['Date'] = pd.to_datetime(dataframe_base['Date'], format="%Y-%m-%d")
    hist_base_shareholding_df = pd.concat([hist_base_shareholding_df, dataframe_base])

    hist_base_shareholding_df_csv = hist_base_shareholding_df
    hist_base_shareholding_df_csv['Date'] = hist_base_shareholding_df_csv['Date'].dt.strftime('%d-%m-%Y')
    hist_base_shareholding_df_csv['Quarter'] = hist_base_shareholding_df_csv['Quarter'].dt.strftime('%b-%y')
    hist_base_shareholding_df_csv.to_csv(PROCESSED_DIR + '\\Historical_base_shareholding_pattern.csv',index=None)

    hist_base_shareholding_df['Quarter'] = pd.to_datetime(hist_base_shareholding_df['Quarter'], format="%b-%y")
    latest_base_shareholding_df = hist_base_shareholding_df.groupby(['Scrip_code']).agg({'Quarter': [np.max]}).reset_index()
    latest_base_shareholding_df.columns = ['Scrip_code', 'Quarter']
    df_merge = pd.merge(latest_base_shareholding_df, hist_base_shareholding_df, on=['Scrip_code', 'Quarter'], how='left')

    df_merge['Total no. shares held'] = df_merge['Total no. shares held'].fillna(0)
    df_merge_csv = df_merge
    df_merge_csv['Quarter'] = pd.to_datetime(df_merge_csv['Quarter'], format='%b-%y')
    df_merge_csv['Date'] = pd.to_datetime(df_merge_csv['Date'], format='%d-%m-%Y')
    df_merge_csv['Quarter'] = df_merge_csv['Quarter'].dt.strftime('%b-%y')
    df_merge_csv['Date'] = df_merge_csv['Date'].dt.strftime('%d-%m-%Y')
    df_merge_csv = (df_merge_csv.drop_duplicates(subset=['Scrip_code', 'Quarter', 'Company', 'Category of shareholder',
                                                         'No. of shareholders', 'Total no. shares held',
                                                         'Shareholding as a % of total no. of shares (calculated as per SCRR, 1957)As a % of (A+B+C2)']))
    df_merge_csv.to_csv(PROCESSED_DIR + '\\latest_base_shareholdings.csv', index=None)

    df_merge_promoter = df_merge[df_merge['Category of shareholder'] == '(A) Promoter & Promoter Group'][
        ['Scrip_code', 'Total no. shares held']]
    df_merge_promoter["Total no. shares held"] = df_merge_promoter["Total no. shares held"].astype(float)
    promoter_df = df_merge_promoter.groupby(['Scrip_code']).agg(
        {'Total no. shares held': ['sum']}).reset_index()
    promoter_df.columns = ['Scrip_code', 'Total_no._shares_held_promoter']

    df_merge_public = df_merge[df_merge['Category of shareholder'] == '(B) Public'][['Scrip_code', 'Total no. shares held']]
    df_merge_public["Total no. shares held"] = df_merge_public["Total no. shares held"].astype(float)
    public_df = df_merge_public.groupby(['Scrip_code']).agg(
        {'Total no. shares held': ['sum']}).reset_index()
    public_df.columns = ['Scrip_code', 'Total_no._shares_held_public']

    df_merge["Total no. shares held"] = df_merge["Total no. shares held"].astype(float)
    sum_all_df = df_merge.groupby(['Scrip_code']).agg(
        {'Total no. shares held': ['sum']}).reset_index()
    sum_all_df.columns = ['Scrip_code', 'Total_no._shares_held_all_sum']

    final_df = pd.merge(promoter_df, public_df, on=['Scrip_code'], how='left')
    final_merged_df = pd.merge(final_df, sum_all_df, on=['Scrip_code'], how='left')
    final_merged_df.to_csv(PROCESSED_DIR + '\\sum_base_shareholdings.csv', index=None)


if __name__ == '__main__':
    try:
        qtr_id, link_quater = get_scriptcode_scraping()
        # qtr_id = 112.00
        # link_quater = 'December%202021'
        call_scraping_job(qtr_id, link_quater)
        execution_status('pass', file, logging.ERROR, TODAY_DATE, 1)

    except Exception as e:
        print(e)
        execution_status(e, file, logging.ERROR, TODAY_DATE, 0)
        sendmail_err(file, e)
    empty_df = pd.DataFrame()
    dfi.export(empty_df, SS_DIR + "\\qtr_Header_tab_fii.png")
    dfi.export(empty_df, SS_DIR + "\\quaterly_Detail_tab_superstar.png")
    exit()
    TODAY_DATE = TODAY_DATE.strftime('%d-%m-%Y')
    File = 'quaterly'

    # call for FII
    try:
        FileType = 'FII'
        bse_shareholdings(FileType, TODAY_DATE, File)
        print('FII')
    except Exception as e:
        print(e)
    # call for superstar
    try:
        FileType = 'Superstar'
        bse_shareholdings(FileType, TODAY_DATE, File)
        print('Superstar')
    except Exception as e:
        print(e)


























