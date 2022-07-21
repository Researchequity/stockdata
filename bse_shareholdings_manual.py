import pandas as pd
import xlwings as xw
from icecream import ic
from filepath import *

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

file_selected = pd.DataFrame()
#input_filepath = "D:\\Program\\bse_shareholding_pattern_scrapping\\bse_shareholding_pattern_scrapping\\daily_data_scrapper\\"
# excel_filepath = "D:\\Program\\Data\\"

daily_data_df = pd.read_csv(PROCESSED_DIR + "\\Historical_detail_ShareHolding_Pattern.csv")
daily_data_df['Date'] = pd.to_datetime(daily_data_df['Date'])
daily_data_df.rename({'Scrip_code': 'Script_code'}, axis=1, inplace=True)

# Historical_data_df = pd.read_csv(input_filepath + "Historical_detail_ShareHolding_Pattern.csv")
# Historical_data_df['Date'] = pd.to_datetime(Historical_data_df['Date'])
# Historical_data_df.rename({'Scrip_code': 'Script_code'}, axis=1, inplace=True)

# data_df = pd.concat([Historical_data_df, daily_data_df])
data_df = daily_data_df

daily_data_df = pd.read_csv(PROCESSED_DIR + "\\latest_detail_shareholdings.csv")

# daily_data_df['Date'] = pd.to_datetime(daily_data_df['Date'])
daily_data_df['Quarter'] = pd.to_datetime(daily_data_df['Quarter'], format='%b-%y')
daily_data_df = daily_data_df[daily_data_df.Quarter == (daily_data_df['Quarter']).max()]
daily_data_df['Quarter'] = daily_data_df['Quarter'].dt.strftime('%b-%y')
daily_data_df.rename({'Scrip_code': 'Script_code'}, axis=1, inplace=True)

data_df.rename({'Total nos. shares held': 'Share', 'Nos. of shareholders': 'Holder',
                    'Nationality': 'Type', 'Category of shareholder': 'Detail',
                    'Shareholding as a % of total no. of shares (calculated as per SCRR, 1957)As a % of (A+B+C2)': 'Sharehold_perct'}, axis=1, inplace=True)

daily_base_df = pd.read_csv(PROCESSED_DIR + "\\Historical_base_shareholding_pattern.csv")
# Historical_base_df = pd.read_csv(input_filepath + "Historical_base_shareholding_pattern.csv")
# data_base_df = pd.concat([Historical_base_df, daily_base_df])
data_base_df =daily_base_df

#data_base_df = data_base_df.drop(['Unnamed: 0'], axis=1)
data_base_df.rename({'Scrip_code': 'Script_code', 'Total no. shares held': 'Share', 'No. of shareholders': 'Holder',
                     'Category of shareholder': 'Category',
                     'Shareholding as a % of total no. of shares (calculated as per SCRR, 1957)As a % of (A+B+C2)': 'Sharehold_perct'},
                    axis=1, inplace=True)

# Input and Output Files
FII = 1
SuperStar = 0

if FII == 1:
    data_excel_file = REPORT_DIR + "\\shareholding_pattern_FII.xlsm"
elif SuperStar == 1:
    data_excel_file = REPORT_DIR + "\\shareholding_pattern_Superstar.xlsm"
else:
    data_excel_file = REPORT_DIR + "\\shareholding_pattern.xlsm"

wb = xw.Book(data_excel_file)
wb_input = wb.sheets("Input")
wb_header = wb.sheets("Header")
wb_detail = wb.sheets("Detail")
wb_base = wb.sheets("Base")

# Input Date
ReportType = (wb.sheets("Input").range('C10').options(index=False).value)
if ReportType == 'FII':
    FII = 1
elif ReportType == 'Superstar':
    SuperStar = 1

Date = wb.sheets("Input").range('C6').options(index=False).value
quarters = wb.sheets("Input").range('C8').options(index=False).value.split(',')
script_code = int(wb.sheets("Input").range('C4').options(index=False).value)
q_num = 1
quarter_qx = []
for q in quarters:
    globals()['q%s' % q_num] = q
    quarter_qx.extend(['q%s' % q_num])
    q_num = q_num + 1

# wb.sheets("Input").range('A9').options(index=False).value = quarters
print(quarters)
print(Date)

# Get Script for date in excel
if Date:
    if Date == 'ALL':
        script_code_df = daily_data_df
        script_code_today = script_code_df['Script_code'].unique()
    else:
        script_code_df = daily_data_df[daily_data_df['Date'] == Date]
        script_code_today = script_code_df['Script_code'].unique()
if data_excel_file == REPORT_DIR + "\\shareholding_pattern.xlsm":
    script_code_today = [script_code]
    print('here')
    script_code_df = daily_data_df[daily_data_df['Script_code'].isin(script_code_today)]


# Get Script code for date in excel
today_company_result = script_code_df.groupby(['Script_code', 'Company']).size()

today_company_result = today_company_result.reset_index()
today_company_result.drop(0, axis=1, inplace=True)

# Output script to Excel
wb_input.range("G2").options(index=False).value = today_company_result

quarter_string =","
data_base_df.dropna(subset = ["Quarter"], inplace=True)
print(data_base_df['Quarter'].unique())
wb_input.range("C13").options(index=False).value = quarter_string.join(data_base_df['Quarter'].unique())


if SuperStar == 1:
    superstar_name_df = pd.read_csv(r"D:\Program\python_ankit\ProjectDir\data" + '\\superstar_name.csv')
    our_superstar = superstar_name_df['Our_SS']
    print(script_code_today)

    data_df = data_df[data_df['Script_code'].isin(script_code_today)]
    data_df['Company'] = data_df['Script_code']

    data_df = data_df[data_df['Category of shareholder'].isin(our_superstar)]
    data_base_df = data_base_df[data_base_df['Script_code'].isin(script_code_today)]
    data_base_df['Company'] = data_base_df['Script_code']
    data_base_df = data_base_df[data_base_df['Category'] == '(B) Public']
elif FII == 1:
    print(script_code_today)
    Category = wb.sheets("Input").range('C11').options(index=False).value
    data_df['Company'] = data_df['Script_code']
    data_df = data_df[data_df['Category'] == Category]  # 'Foreign Portfolio Investors'
    data_base_df['Company'] = data_base_df['Script_code']
    filt = str(wb.sheets("Input").range('C12').options(index=False).value)
    data_base_df = data_base_df[data_base_df['Category'] == filt]   # (A) Promoter & Promoter Group
    # data_base_df = data_base_df[data_base_df['Category'] == '(A) Promoter & Promoter Group']

else:
    data_df = data_df[data_df['Script_code'].isin(script_code_today)]
    data_base_df = data_base_df[data_base_df['Script_code'].isin(script_code_today)]
    data_base_df['Company'] = data_base_df['Script_code']

# Hard Code Input Quarter
data_df = data_df[data_df['Quarter'].isin(quarters)]
data_base_df = data_base_df[data_base_df['Quarter'].isin(quarters)]

# to arrange quarters in desc after pivtoed
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
if SuperStar != 1:
    data_header_df = data_df[data_df['Category'] == data_df['Detail']]
    data_header_df.drop('Company', axis=1, inplace=True)
    data_header_df = pd.merge(today_company_result, data_header_df, on=['Script_code'])
    data_header_df.drop_duplicates(
        subset=["Script_code", "Company", "Quarter", "Type", "Detail", "Holder", "Header", "Share"], keep=False,
        inplace=True)
    data_header_df = data_header_df.pivot(index=['Script_code', 'Company', 'Type', 'Category'], columns='Quarter',
                                          values=['Sharehold_perct', 'Share'])
    data_header_df.columns = list(map("_".join, data_header_df.columns))
    data_header_df.to_csv(RAW_DIR + '\\stage_fii_comp.csv')
    data_header_csv = pd.read_csv(RAW_DIR + '\\stage_fii_comp.csv')

    for i in range(len(quarters)-1):
        print(quarters)
        i=i+1
        print(i)
        data_header_csv['diff_share_%_q' + str(i) + 'q' + str(i + 1)] = round(
            (data_header_csv['Sharehold_perct_q' + str(i)] - data_header_csv['Sharehold_perct_q' + str(i + 1)]) * 100 /
            data_header_csv['Sharehold_perct_q' + str(i + 1)], 2)

    wb_header.range("A1").options(index=None).value = data_header_csv

# Get Detail Data
data_detail_df = data_df[data_df['Category'] != data_df['Detail']]
data_detail_df.drop('Company', axis=1, inplace=True)
data_detail_df = pd.merge(today_company_result, data_detail_df, on=['Script_code'])
data_detail_df.drop_duplicates(subset=["Script_code", "Company", "Type", "Detail", "Quarter"], keep=False, inplace=True)
data_detail_df = data_detail_df.pivot(index=['Script_code', 'Company', 'Type', 'Category', 'Detail'], columns='Quarter',
                                      values=['Sharehold_perct', 'Share'])
data_detail_df.columns = list(map("_".join, data_detail_df.columns))
# print to sheet
wb_detail.range("A1").options().value = data_detail_df

# to check fles compared
if FII == 0:
    file_selected.to_csv("D:\\Program\\Data\\Bse_ShareHolding_Files\\" + "_" + str(script_code) + ".check")


