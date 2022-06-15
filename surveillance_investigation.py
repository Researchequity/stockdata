import requests
from utils import *
import dataframe_image as dfi

file = os.path.basename(__file__)
date_today = datetime.date.today()  # - datetime.timedelta(days=1)
date_today, prev_date, fut_date = get_market_prev_date_fut_date(date_today)

today_date = date_today.strftime('%d%m%Y')
yest_date = prev_date.strftime('%d%m%Y')
next_date = fut_date.strftime('%d%m%Y')
fii_derivatives_date = date_today.strftime('%d-%b-%Y')


def surveillance_investigation():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9", "Accept-Encoding": "gzip, deflate, br"}
    s = requests.session()
    s.get('https://www.nseindia.com/get-quotes/derivatives?symbol=' + 'NIFTY', headers=headers)
    r = s.get('https://www.nseindia.com/api/circulars', headers=headers).json()

    surv_df_all = pd.DataFrame()

    for i in range(len(r['data'])):
        temp_df = pd.DataFrame([r['data'][i]])
        temp_df = temp_df[
            ['fileDept', 'circNumber', 'fileExt', 'sub', 'cirDate', 'cirDisplayDate', 'circFilename',
             'circFilelink', 'circCompany', 'circDisplayNo', 'circCategory', 'circDepartment']]
        surv_df_all = pd.concat([surv_df_all, temp_df])
    surv_df_all.to_csv(r'\\192.168.41.190\ashish\surveillance_investigation_all_data.csv')
    surv_df_all['cirDate'] = pd.to_datetime(surv_df_all['cirDate'], format='%Y%m%d')
    surv_df_all = surv_df_all[surv_df_all.fileDept == 'SURV']
    surv_df_all = surv_df_all[surv_df_all.fileExt != 'pdf']
    surv_IBC_df = surv_df_all.loc[surv_df_all['sub'].str.contains("IBC", case=False)]

    surv_df_all = surv_df_all.loc[surv_df_all['sub'].str.contains("ASM", case=False)]
    surv_df_all['sub_test'] = surv_df_all['sub'].str.split('(')
    surv_df_all[['sub1', 'sub2']] = pd.DataFrame(surv_df_all['sub_test'].tolist(), index=surv_df_all.index)
    surv_df_all['sub2'] = surv_df_all['sub2'].str.replace(')', '', regex=True)
    surv_df_all['sub2'] = surv_df_all['sub2'].str.strip('sub2 ')

    filt1 = surv_df_all[surv_df_all.sub2 == 'ASM']
    filt1 = filt1[filt1['cirDate'] == filt1['cirDate'].max()]  # - datetime.timedelta(days=1)
    filt2 = surv_df_all[surv_df_all.sub2 == 'ST-ASM']
    filt2 = filt2[filt2['cirDate'] == filt2['cirDate'].max()]  # - datetime.timedelta(days=1)

    surv_IBC_df = surv_IBC_df[surv_IBC_df['cirDate'] == surv_IBC_df['cirDate'].max()]

    get_zip_link_one = filt1['circFilelink'][0]
    get_zip_link_two = filt2['circFilelink'][0]
    get_zip_link_three = surv_IBC_df['circFilelink'][0]

    # calculation and extraction of ASM data
    response = requests.get(get_zip_link_one, timeout=1)
    open(RAW_DIR + '\\get_zip_link_one.zip', 'wb').write(response.content)
    zf = ZipFile(RAW_DIR + "\\get_zip_link_one.zip")
    zf.extractall(RAW_DIR)
    zf.extractall(r'\\192.168.41.190\nilesh')
    zf.close()
    asm_df = pd.read_excel(open(RAW_DIR + '\\Annexure.xlsx', 'rb'), sheet_name='Consolidated - ASM', header=None)
    asm_df.drop(asm_df.head(3).index, inplace=True)
    asm_df.drop([0], inplace=True, axis=1)
    asm = asm_df.rename(columns={1: "Symbol", 2: "Security_Name", 3: "ISIN", 4: "Stage"})
    asm['date_today'] = filt1['cirDate'][0]
    asm['sub'] = 'ASM'
    asm['date_today'] = pd.to_datetime(asm['date_today'], format='%Y-%m-%d')
    asm_df_tab1 = pd.read_excel(open(RAW_DIR + '\\Annexure.xlsx', 'rb'), sheet_name='Annexure I', header=None)
    asm_df_tab1 = asm_df_tab1.fillna('-')
    # asm_df_tab1 = asm_df_tab1.style.background_gradient()
    asm_df_tab1 = asm_df_tab1.style.hide_index()
    dfi.export(asm_df_tab1, SS_DIR + "\\asm_df_tab1.png")
    asm_df_tab2 = pd.read_excel(open(RAW_DIR + '\\Annexure.xlsx', 'rb'), sheet_name='Annexure II', header=None)
    asm_df_tab2 = asm_df_tab2.fillna('-')
    # asm_df_tab2 = asm_df_tab2.style.background_gradient()
    asm_df_tab2 = asm_df_tab2.style.hide_index()
    dfi.export(asm_df_tab2, SS_DIR + "\\asm_df_tab2.png")

    # calculation and extraction of ST-ASM data
    response = requests.get(get_zip_link_two, timeout=1)
    open(RAW_DIR + '\\get_zip_link_two.zip', 'wb').write(response.content)
    zf = ZipFile(RAW_DIR + "\\get_zip_link_two.zip")
    zf.extractall(RAW_DIR)
    zf.extractall(r'\\192.168.41.190\nilesh')
    zf.close()

    st_asm_df_tab1 = pd.read_excel(open(RAW_DIR + '\\Annexure_ST.xlsx', 'rb'), sheet_name='Annexure I', header=None)
    st_asm_df_tab1 = st_asm_df_tab1.fillna('-')
    # st_asm_df_tab1 = st_asm_df_tab1.style.background_gradient()
    st_asm_df_tab1 = st_asm_df_tab1.style.hide_index()
    dfi.export(st_asm_df_tab1, SS_DIR + "\\st_asm_df_tab1.png")
    st_asm_df_tab2 = pd.read_excel(open(RAW_DIR + '\\Annexure_ST.xlsx', 'rb'), sheet_name='Annexure II', header=None)
    st_asm_df_tab2 = st_asm_df_tab2.fillna('-')
    # st_asm_df_tab2 = st_asm_df_tab2.style.background_gradient()
    st_asm_df_tab2 = st_asm_df_tab2.style.hide_index()
    dfi.export(st_asm_df_tab2, SS_DIR + "\\st_asm_df_tab2.png")
    st_asm_df_tab3 = pd.read_excel(open(RAW_DIR + '\\Annexure_ST.xlsx', 'rb'), sheet_name='Consolidated - ST ASM',
                                   header=None)
    st_asm_df_tab3 = st_asm_df_tab3.fillna('-')
    # st_asm_df_tab3 = st_asm_df_tab3.style.background_gradient()
    st_asm_df_tab3 = st_asm_df_tab3.style.hide_index()
    dfi.export(st_asm_df_tab3, SS_DIR + "\\st_asm_df_tab3.png")

    st_asm_df = pd.read_excel(open(RAW_DIR + '\\Annexure_ST.xlsx', 'rb'), sheet_name='Consolidated - ST ASM',
                              header=None)
    st_asm_df.drop(st_asm_df.head(3).index, inplace=True)
    st_asm_df.drop([0], inplace=True, axis=1)
    st_asm = st_asm_df.rename(columns={1: "Symbol", 2: "Security_Name", 3: "ISIN", 4: "Stage"})
    st_asm['date_today'] = filt2['cirDate'][0]
    st_asm['sub'] = 'ST-ASM'
    st_asm['date_today'] = pd.to_datetime(st_asm['date_today'], format='%Y-%m-%d')

    # merge ASM and ST_ASM to surveillance_Invest_hist file
    if not os.path.exists(PROCESSED_DIR + '\\surveillance_Invest_hist.csv'):
        st_asm['date_today'] = st_asm['date_today'].dt.strftime('%d-%m-%Y')
        st_asm.to_csv(PROCESSED_DIR + '\\surveillance_Invest_hist.csv', index=None)
    else:
        Historical = pd.read_csv(PROCESSED_DIR + '\\surveillance_Invest_hist.csv')
        Historical['date_today'] = pd.to_datetime(Historical['date_today'], format='%d-%m-%Y')
        if st_asm['date_today'].max() != Historical['date_today'].max():
            Historical = pd.concat([st_asm, Historical])
            Historical = pd.concat([asm, Historical])
            Historical = Historical.dropna(subset=['Symbol'])
            Historical['date_today'] = Historical['date_today'].dt.strftime('%d-%m-%Y')
            Historical.to_csv(PROCESSED_DIR + '\\surveillance_Invest_hist.csv', index=False)

    # calculation and extraction of IBC data
    response = requests.get(get_zip_link_three, timeout=1)
    open(RAW_DIR + '\\get_zip_link_three.zip', 'wb').write(response.content)
    zf = ZipFile(RAW_DIR + "\\get_zip_link_three.zip")
    zf.extractall(RAW_DIR)
    zf.close()
    ibc_tab1 = pd.read_excel(open(RAW_DIR + '\\Annexure.xlsx', 'rb'), sheet_name='Annexure I', header=None)
    ibc_tab1 = ibc_tab1.fillna('-')
    # ibc_tab1 = ibc_tab1.style.background_gradient()
    ibc_tab1 = ibc_tab1.style.hide_index()
    dfi.export(ibc_tab1, SS_DIR + "\\ibc_tab1.png")

    ibc_tab2 = pd.read_excel(open(RAW_DIR + '\\Annexure.xlsx', 'rb'), sheet_name='Annexure II', header=None)
    ibc_tab2 = ibc_tab2.fillna('-')
    # ibc_tab2 = ibc_tab2.style.background_gradient()
    ibc_tab2 = ibc_tab2.style.hide_index()
    dfi.export(ibc_tab2, SS_DIR + "\\ibc_tab2.png")

    ibc_df = pd.read_excel(open(RAW_DIR + '\\Annexure.xlsx', 'rb'), sheet_name='Consolidated - ASM (IBC)',
                              header=None)
    ibc_df.drop(ibc_df.head(2).index, inplace=True)
    ibc_df.drop([0], inplace=True, axis=1)
    ibc_df = ibc_df.rename(columns={1: "Symbol", 2: "Security_Name", 3: "ISIN", 4: "Stage"})
    ibc_df['date_today'] = surv_IBC_df['cirDate'][0]
    ibc_df['sub'] = 'IBC'
    ibc_df['date_today'] = pd.to_datetime(ibc_df['date_today'], format='%Y-%m-%d')

    if not os.path.exists(PROCESSED_DIR + '\\surveillance_ibc.csv'):
        ibc_df['date_today'] = ibc_df['date_today'].dt.strftime('%d-%m-%Y')
        ibc_df.to_csv(PROCESSED_DIR + '\\surveillance_ibc.csv', index=None)
    else:
        Historical = pd.read_csv(PROCESSED_DIR + '\\surveillance_ibc.csv')
        Historical['date_today'] = pd.to_datetime(Historical['date_today'], format='%d-%m-%Y')
        if ibc_df['date_today'].max() != Historical['date_today'].max():
            Historical = pd.concat([ibc_df, Historical])
            Historical = Historical.dropna(subset=['Symbol'])
            Historical['date_today'] = Historical['date_today'].dt.strftime('%d-%m-%Y')
            Historical.to_csv(PROCESSED_DIR + '\\surveillance_ibc.csv', index=False)


if __name__ == '__main__':
    try:
        print('Surveillance_Investigation')
        surveillance_investigation()
        execution_status('pass', file, logging.ERROR, date_today, 1)
    except Exception as e:
        print('err', str(e))
        execution_status(str(e), file, logging.ERROR, date_today, 0)
        sendmail_err(file, e)
        pass

    # Historical File
    # link_csv = pd.read_csv(RAW_DIR + '\\circular.csv')[['DATE', 'SUBJECT', 'LINK']]
    # link_csv['DATE'] = pd.to_datetime(link_csv['DATE'], format='%B %d, %Y')
    # # link_csv['SUBJECT'] = link_csv['SUBJECT'].strip()
    # unique_dates = link_csv['DATE'].unique()#.astype(str)
    # print(unique_dates)
    #
    # # link_csv = link_csv.columns.str.strip()
    # link_csv.columns = link_csv.columns.str.replace(' ', '')
    #
    # for x in unique_dates:
    #     ic(x)
    #     t = pd.to_datetime(str(x))
    #     str_date = t.strftime('%d-%m-%Y')
    #     str_date = pd.to_datetime(str_date, format='%d-%m-%Y')

    # filt1 = link_csv[link_csv.SUBJECT == 'ASM']
    # filt1 = filt1[filt1['DATE'] == str_date]
    # ic(filt1)
    #
    # index_val_one = int(filt1.index.values)
    # get_zip_link_one = filt1['LINK'][index_val_one]

    # filt2 = link_csv[link_csv.SUBJECT == 'ST-ASM']
    # filt2 = filt2[filt2['DATE'] == str_date]
    # ic(filt2)
    #
    # index_val = int(filt2.index.values)
    # get_zip_link_two = filt2['LINK'][index_val]
    # ic(get_zip_link_one)
    # ic(get_zip_link_two)

    # response = requests.get(get_zip_link_one, timeout=5)
    # open(RAW_DIR + '\\get_zip_link_one.zip', 'wb').write(response.content)
    # zf = ZipFile(RAW_DIR + "\\get_zip_link_one.zip")
    # zf.extractall(RAW_DIR)
    # zf.close()

    # response = requests.get(get_zip_link_two, timeout=5)
    # open(RAW_DIR + '\\get_zip_link_two.zip', 'wb').write(response.content)
    # zf = ZipFile(RAW_DIR + "\\get_zip_link_two.zip")
    # zf.extractall(RAW_DIR)
    # zf.close()

    # asm_df = pd.read_excel(open(RAW_DIR + '\\Annexure.xlsx', 'rb'), sheet_name='Consolidated - ASM', header=None)
    # asm_df.drop(asm_df.head(3).index, inplace=True)
    # asm_df.drop([0], inplace=True, axis=1)
    # asm = asm_df.rename(columns={1: "Symbol", 2: "Security_Name", 3: "ISIN", 4: "Stage"})
    # asm['date_today'] = filt1['DATE'][index_val_one]
    # asm['sub'] = filt1['SUBJECT'][index_val_one]
    # asm['date_today'] = pd.to_datetime(asm['date_today'], format='%Y-%m-%d')

    # st_asm_df = pd.read_excel(open(RAW_DIR + '\\Annexure_ST.xlsx', 'rb'), sheet_name='Consolidated - ST ASM',
    #                           header=None)
    # st_asm_df.drop(st_asm_df.head(3).index, inplace=True)
    # st_asm_df.drop([0], inplace=True, axis=1)
    # st_asm = st_asm_df.rename(columns={1: "Symbol", 2: "Security_Name", 3: "ISIN", 4: "Stage"})
    # st_asm['date_today'] = filt2['DATE'][index_val]
    # st_asm['sub'] = filt2['SUBJECT'][index_val]
    # st_asm['date_today'] = pd.to_datetime(st_asm['date_today'], format='%Y-%m-%d')
    # st_asm.to_csv(RAW_DIR + '\\st_asm.csv', index=False)
    # if not os.path.exists(PROCESSED_DIR + '\\surveillance_Invest_devlopment.csv'):
    #     st_asm['date_today'] = st_asm['date_today'].dt.strftime('%d-%m-%Y')
    #     st_asm.to_csv(PROCESSED_DIR + '\\surveillance_Invest_devlopment.csv', index=None)
    # else:
    #     Historical = pd.read_csv(PROCESSED_DIR + '\\surveillance_Invest_devlopment.csv')
    #     Historical['date_today'] = pd.to_datetime(Historical['date_today'], format='%d-%m-%Y')
    #     if st_asm['date_today'].max() != Historical['date_today'].max():
    #         Historical = pd.concat([st_asm, Historical])
    #         Historical = pd.concat([st_asm, Historical])
    #         Historical = Historical.dropna(subset=['Symbol'])
    #         Historical['date_today'] = Historical['date_today'].dt.strftime('%d-%m-%Y')
    #         Historical.to_csv(PROCESSED_DIR + '\\surveillance_Invest_devlopment.csv', index=False)
    #         exit()
