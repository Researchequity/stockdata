import xlwings as xw
from datetime import datetime as dt
import dataframe_image as dfi
from utils import *
pd.options.mode.chained_assignment = None  # default='warn'

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def bse_shareholdings(FileType, TODAY_DATE, File):
    try:
        Historical_data_df = pd.read_csv(PROCESSED_DIR + "\\Historical_detail_ShareHolding_Pattern.csv")
        Historical_data_df['Date'] = pd.to_datetime(Historical_data_df['Date'], format='%d-%m-%Y')
        Historical_data_df.rename({'Scrip_code': 'Script_code'}, axis=1, inplace=True)

        data_df = Historical_data_df

        daily_data_df = pd.read_csv(PROCESSED_DIR + "\\latest_detail_shareholdings.csv")
        daily_data_df['Date'] = pd.to_datetime(daily_data_df['Date'])
        daily_data_df.rename({'Scrip_code': 'Script_code'}, axis=1, inplace=True)

        data_df.rename({'Total nos. shares held': 'Share', 'Nos. of shareholders': 'Holder',
                        'Nationality': 'Type', 'Category of shareholder': 'Detail',
                        'Shareholding as a % of total no. of shares (calculated as per SCRR, 1957)As a % of (A+B+C2)': 'Sharehold_perct'}, axis=1, inplace=True)

        Historical_base_df = pd.read_csv(PROCESSED_DIR + "\\Historical_base_shareholding_pattern.csv")
        Historical_base_df['Date'] = pd.to_datetime(Historical_base_df['Date'], format='%d-%m-%Y')
        data_base_df = Historical_base_df
        data_base_df.rename({'Scrip_code': 'Script_code', 'Total no. shares held': 'Share', 'No. of shareholders': 'Holder',
                             'Category of shareholder': 'Category',
                             'Shareholding as a % of total no. of shares (calculated as per SCRR, 1957)As a % of (A+B+C2)': 'Sharehold_perct'},
                            axis=1, inplace=True)

        # Input and Output Files
        if FileType == 'FII':
            data_excel_file = REPORT_DIR + "\\shareholding_pattern_FII.xlsm"
        elif FileType == 'Superstar':
            data_excel_file = REPORT_DIR + "\\shareholding_pattern_Superstar.xlsm"
        else:
            data_excel_file = REPORT_DIR + "\\shareholding_pattern.xlsm"



        wb = xw.Book(data_excel_file)
        wb_input = wb.sheets("Input")
        wb_header = wb.sheets("Header")
        wb_detail = wb.sheets("Detail")
        wb_base = wb.sheets("Base")
        wb_input.range('G3:H100').clear()
        wb_base.range('A2:M100').clear()
        wb_detail.range('A2:K100').clear()
        wb_header.range('A2:L100').clear()
        wb.save(data_excel_file)


        script_code = int(wb.sheets("Input").range('C4').options(index=False).value)


        print(TODAY_DATE)
        TODAY_DATE = pd.to_datetime(TODAY_DATE, format='%d-%m-%Y')

        # Get Script for date in excel
        if TODAY_DATE:
            if TODAY_DATE == 'ALL':
                script_code_df = daily_data_df
                script_code_today = script_code_df['Script_code'].unique()
            elif File == 'intermittent':
                script_code_df = pd.read_csv(RAW_DIR + "\\Today_intermedent_Script_" + str(dt.today().date()) + ".csv")
                script_code_df = script_code_df.rename(columns={"Security Code": "Script_code", "Security Name": "Company"})
                script_code_today = script_code_df['Script_code'].unique()
            elif File == 'quaterly':
                script_code_df = pd.read_csv(RAW_DIR + "\\Today_Script_" + str(dt.today().date()) + ".csv")
                script_code_df = script_code_df.rename(columns={"Security Code": "Script_code", "Security Name": "Company"})
                script_code_today = script_code_df['Script_code'].unique()
            else:
                script_code_df = Historical_data_df[Historical_data_df['Date'] == TODAY_DATE]
                script_code_today = script_code_df['Script_code'].unique()

        if data_excel_file == REPORT_DIR + "\\shareholding_pattern.xlsm":
            script_code_today = [script_code]
            script_code_df = daily_data_df[daily_data_df['Script_code'].isin(script_code_today)]

        # Get Script code for date in excel
        today_company_result = script_code_df.groupby(['Script_code', 'Company']).size()
        today_company_result = today_company_result.reset_index()
        today_company_result.drop(0, axis=1, inplace=True)

        # Output script to Excel
        wb_input.range("G2").options(index=False).value = today_company_result

        quarter_string =","
        #drop if nan value exist
        data_base_df.dropna(subset = ["Quarter"], inplace=True)
        print(data_base_df['Quarter'].unique())
        wb_input.range("C13").options(index=False).value = quarter_string.join(data_base_df['Quarter'].unique())



        if FileType == 'Superstar':
            data_df = data_df[data_df['Script_code'].isin(script_code_today)]
            data_df['Company'] = data_df['Script_code']

            data_df['lower_names'] = data_df['Detail'].str.lower()
            data_df = data_df[data_df['lower_names'].notna()]
            data_df = superstar_check(data_df)
            data_base_df = data_base_df[data_base_df['Script_code'].isin(script_code_today)]

            data_base_df['Company'] = data_base_df['Script_code']
            data_base_df = data_base_df[data_base_df['Category'] == '(B) Public']

        elif FileType == 'FII':
            print(script_code_today)
            Category = (wb.sheets("Input").range('C11').options(index=False).value)
            #data_df = data_df[data_df['Script_code'].isin(script_code_today)]
            data_df['Company'] = data_df['Script_code']
            data_df = data_df[data_df['Category'] == Category] #'Foreign Portfolio Investors'
            data_base_df = data_base_df[data_base_df['Script_code'].isin(script_code_today)]
            data_base_df['Company'] = data_base_df['Script_code']
            data_base_df = data_base_df[data_base_df['Category'] == '(B) Public'] #(A) Promoter & Promoter Group
        elif FileType == 'Promoter':
            print(script_code_today)
            Category = (wb.sheets("Input").range('C11').options(index=False).value)
            data_df = data_df[data_df['Script_code'].isin(script_code_today)]
            data_df['Company'] = data_df['Script_code']
            data_df = data_df[data_df['Category'] == Category] #'Foreign Portfolio Investors'
            data_base_df = data_base_df[data_base_df['Script_code'].isin(script_code_today)]
            data_base_df['Company'] = data_base_df['Script_code']
            data_base_df = data_base_df[data_base_df['Category'] == '(A) Promoter & Promoter Group']
        else:
            data_df = data_df[data_df['Script_code'].isin(script_code_today)]
            data_base_df = data_base_df[data_base_df['Script_code'].isin(script_code_today)]
            data_base_df['Company'] = data_base_df['Script_code']

        # get max 3 Quarters
        # df = pd.DataFrame()
        # df['Quarter'] = data_base_df['Quarter'].unique()
        # df['Quarter'] = pd.to_datetime(df['Quarter'], format='%b-%y')
        # df.sort_values(by=['Quarter'], ascending=False, inplace=True)
        # df = df.head(3)
        # df['Quarter'] = df['Quarter'].dt.strftime('%b-%y')
        # df = df['Quarter'].tolist()
        # wb_input.range("C8").options(index=False).value = quarter_string.join(df)

        quarters = (wb.sheets("Input").range('C8').options(index=False).value).split(',')
        q_num = 1
        quarter_qx = []
        for q in quarters:
            globals()['q%s' % q_num] = q
            quarter_qx.extend(['q%s' % q_num])
            q_num = q_num + 1


        # Hard Code Input Quarter
        data_df = data_df[data_df['Quarter'].isin(quarters)]
        data_base_df = data_base_df[data_base_df['Quarter'].isin(quarters)]

        # to arrange quarters in desc after pivoted
        data_df.replace({'Quarter': {q1: 'q1', q2: 'q2', q3: 'q3'}}, inplace=True)
        data_base_df.replace({'Quarter': {q1: 'q1', q2: 'q2', q3: 'q3'}}, inplace=True)

        # Get Base Data
        data_base_df.drop_duplicates(subset =["Script_code","Company","Quarter","Category"],keep = False, inplace = True)
        data_base_df = data_base_df.pivot(index=['Script_code', 'Company', 'Category'], columns='Quarter',values=['Share', 'Holder', 'Sharehold_perct'])
        data_base_df.columns = list(map("_".join, data_base_df.columns))
        data_base_df = data_base_df.reset_index()
        data_base_df.drop('Company', axis=1, inplace=True)
        data_base_df = pd.merge(today_company_result, data_base_df, on=['Script_code'])
        wb_base.range("A1").options().value = data_base_df



        # Get Header Data
        if FileType != 'Superstar':
            data_header_df = data_df[data_df['Category'] == data_df['Detail']]
            data_header_df.drop('Company', axis=1, inplace=True)
            data_header_df = pd.merge(today_company_result, data_header_df, on=['Script_code'])
            data_header_df.drop_duplicates(subset=["Script_code", "Company", "Quarter", "Type", "Detail", "Holder", "Header", "Share"], keep=False,inplace=True)
            data_header_df = data_header_df.pivot(index=['Script_code', 'Company', 'Type', 'Category'], columns='Quarter',values=['Sharehold_perct', 'Share'])
            data_header_df.columns = list(map("_".join, data_header_df.columns))
            data_header_df.to_csv(METADATA_DIR + '\\stage_fii_comp.csv')
            data_header_csv = pd.read_csv(METADATA_DIR + '\\stage_fii_comp.csv')


            for i in range(len(quarters)-1):
                print(quarters)
                i=i+1
                print(i)
                data_header_csv['diff_share_%_q'+str(i)+'q'+str(i+1)] = round((data_header_csv['Sharehold_perct_q'+str(i)] - data_header_csv['Sharehold_perct_q'+str(i+1)])* 100 / data_header_csv['Sharehold_perct_q'+str(i+1)],2)
                #data_header_csv = data_header_csv[data_header_csv['Share_q1'] != data_header_csv['Share_q3']]

            wb_header.range("A1").options(index=None).value = data_header_csv

            data_header_csv = data_header_csv.head(50)
            data_header_csv = data_header_csv.fillna(0)

            # data_header_csv = data_header_csv.style.background_gradient()  # adding a gradient based on values in cell
            if File == 'quaterly':
                dfi.export(data_header_csv, SS_DIR + "\\qtr_Header_tab_fii.png")
            elif File == 'intermittent':
                dfi.export(data_header_csv, SS_DIR + "\\Header_tab_fii.png")

        # Get Detail Data
        data_detail_df = data_df[data_df['Category'] != data_df['Detail']]
        data_detail_df.drop('Company', axis=1, inplace=True)
        data_detail_df = pd.merge(today_company_result, data_detail_df, on=['Script_code'])
        data_detail_df.drop_duplicates(subset=["Script_code", "Company", "Type", "Detail", "Quarter"],keep=False, inplace=True)
        data_detail_df = data_detail_df.pivot(index=['Script_code', 'Company', 'Type', 'Category', 'Detail'], columns='Quarter', values=['Sharehold_perct', 'Share'])
        data_detail_df.columns = list(map("_".join, data_detail_df.columns))


        # print to sheet
        wb_detail.range("A1").options().value = data_detail_df
        data_detail_df = data_detail_df.head(50)
        data_detail_df = data_detail_df.fillna(0)

        if (File == 'quaterly') and (FileType == 'Superstar'):
            dfi.export(data_detail_df, SS_DIR + "\\quaterly_Detail_tab_superstar.png")
        elif File == 'intermittent':
            dfi.export(data_detail_df, SS_DIR + "\\Detail_tab_superstar.png")

        wb.save(data_excel_file)
        ic('success')
        app = xw.apps.active
        app.quit()
    except Exception as e:
        print(e)
        app = xw.apps.active
        app.quit()


if __name__ == '__main__':
    FileType = 'FII'
    TODAY_DATE = datetime.datetime(2021, 9, 10)
    TODAY_DATE = TODAY_DATE.strftime('%d-%m-%Y')
    File = 'quaterly' #'intermittent'
    bse_shareholdings(FileType, TODAY_DATE, File)