from utils import *
import pandas as pd
import numpy as np
import xlwings as xw
import re

chayan_dir = r'\\192.168.41.190\chayan'
REPORT_DIR = r'\\192.168.41.190\report'
today_date = datetime.date.today() - datetime.timedelta(days=29)
today_date = today_date.strftime('_%m_%Y')


def mutual_fund():
    file = pd.read_excel(chayan_dir + '\\MF ROHAN\\Mutual_Fund' + today_date + '.xlsx',
                         sheet_name="Query Output1", skiprows=range(0, 4), header=None, index_col=None)
    final_df = file.reset_index()

    final_df.columns = ['0', 'ISIN', 'NSE Symbol', 'BSE Code', 'Instrument Name', 'AMC Name', 'Scheme Name',
                        'Company s Mkt Cap (Cr.)', 'Market Value (Cr.)_prev', 'No. of Shares_prev',
                        'Market Value (Cr.)_current', 'No. of Shares_current'
                        ]
    final_df.drop(final_df.tail(4).index, inplace=True)
    final_df.drop(['0'], inplace=True, axis=1)
    final_df = pd.DataFrame(final_df)
    final_df = final_df.fillna(0)

    # filters
    # final_df = final_df[final_df['Market Value (Cr.)_current'] != 0]
    # final_df = final_df[final_df['Company s Mkt Cap (Cr.)'] != 0]
    # final_df = final_df[final_df['No. of Shares_current'] != 0]
    # final_df = final_df[final_df['No. of Shares_prev'] != 0]

    # calculative part
    final_df['Month Change in Shares%'] = ((final_df['No. of Shares_current'] - final_df['No. of Shares_prev']) * 100) / \
                                          final_df['No. of Shares_prev']
    final_df['Month Change in Shares%'] = final_df['Month Change in Shares%'].astype(str).replace('inf', 100)
    final_df['Month Change'] = final_df['No. of Shares_current'] - final_df['No. of Shares_prev']
    final_df['Change in value $'] = final_df['Market Value (Cr.)_current'] - final_df['Market Value (Cr.)_prev']
    final_df = final_df.round({"% of Total Holding": 2})

    stock_metadata = pd.read_csv(METADATA_DIR + '\\StockMetadata.csv')[['ISIN', 'Sector', 'MarketCap', 'issued_shares']]
    # final_df['ISIN'] = final_df['ISIN'].astype(str)

    df_merge = pd.merge(final_df, stock_metadata, on=['ISIN'], how='left')
    df_merge = df_merge.rename(columns={"Instrument Name": "Invested In", "Market Value (Cr.)_current": "latest Value",
                                        "No. of Shares_current": "Quantity", "Scheme Name": "Fund_Name"
        , "MarketCap": "CAP"})
    df_merge = df_merge.fillna(0)
    df_merge['issued_shares'] = df_merge['issued_shares'].replace('-', 0).astype(float)
    df_merge['% of Total Holding'] = (df_merge['Quantity'] / df_merge['issued_shares']) * 100
    df_merge['% of Total Holding_prev'] = (df_merge['No. of Shares_prev'] / df_merge['issued_shares']) * 100
    df_merge['change in % of equity'] = df_merge['% of Total Holding'] - df_merge['% of Total Holding_prev']


    df_merge['prev_hold_val_today'] = (
                (df_merge['latest Value'] / df_merge['Quantity']) * df_merge['No. of Shares_prev']).fillna(0)
    df_merge['prev_hold_val_today'] = np.where(df_merge['Quantity'] == 0, df_merge['Market Value (Cr.)_prev'],
                                               df_merge['prev_hold_val_today'])
    df_merge['chng_val_asof_current_price'] = df_merge['latest Value'] - df_merge['prev_hold_val_today']
    df_merge = df_merge[
        ["Invested In", "Sector", "latest Value", "% of Total Holding", "Quantity", "Month Change in Shares%",
         "Month Change", "Change in value $", "Fund_Name", "CAP", "ISIN", "NSE Symbol", "BSE Code", "AMC Name",
         "Company s Mkt Cap (Cr.)", "Market Value (Cr.)_prev", "No. of Shares_prev", "issued_shares",
         "% of Total Holding_prev", "change in % of equity", "prev_hold_val_today", "chng_val_asof_current_price"]]
    df_merge.to_csv(REPORT_DIR + '\\MF FINAL\\Mutual_Fund' + today_date + '_processed.csv', index=None)

    file = REPORT_DIR + "\\MF FINAL\\Analysis_Mutual_Fund_Template.xlsx"
    wb = xw.Book(file)
    sheet_oi_single = wb.sheets('Aditya_Birla_Sun_Life_Small_Cap')
    sheet_oi_single.clear()
    sheet_oi_single.range("A1").options(index=None).value = df_merge
    ic(df_merge.head(5))
    wb.save()
    wb.close()


def mutual_fund_analysis():
    file = REPORT_DIR + "\\MF FINAL\\Analysis_Mutual_Fund_Template.xlsx"
    sheet = pd.read_excel(file, sheet_name='Aditya_Birla_Sun_Life_Small_Cap')

    # FINAL TASK 1
    sheet1 = sheet[['ISIN', 'Quantity', 'Month Change', '% of Total Holding', 'Invested In']]
    group = pd.pivot_table(sheet1, index=['ISIN', 'Invested In'],
                           values=['Quantity', 'Month Change', '% of Total Holding'],
                           margins=True, margins_name="Grand Total", aggfunc=np.sum)
    group.rename(columns={'Quantity': 'Sum of Quantity', 'Month Change': 'Sum of Month Change',
                          '% of Total Holding': 'Sum of % Total Holding'}, inplace=True)
    group = group[group['Sum of Quantity'] != 0]
    group = group[group['Sum of % Total Holding'] != np.inf]
    group = group.sort_values(by='Sum of % Total Holding', ascending=False)
    group['change'] = group['Sum of Month Change'] / (group['Sum of Quantity'] + abs(group['Sum of Month Change']))
    group['change'] = group['change'] * 100
    group['change'] = group['change'].round(decimals=2)
    group['Sum of % Total Holding'] = group['Sum of % Total Holding'].round(decimals=2)
    group = group[(group['Sum of % Total Holding'] > 1.00) & ((group['change'] > 10) | (group['change'] < -10))]
    group = group.reset_index()
    group = group[['ISIN', 'Invested In', 'Sum of Quantity', 'Sum of Month Change', 'Sum of % Total Holding', 'change']]

    # FINAL TASK 2
    sheet['chng_val_asof_current_price'] = round(sheet['chng_val_asof_current_price'])
    sheet = sheet[(sheet['chng_val_asof_current_price']>0.5) | (sheet['chng_val_asof_current_price']<-0.5)]
    sheet2 = sheet[['CAP', 'AMC Name', 'Invested In', 'chng_val_asof_current_price']]
    CapPattern = re.compile('Small|Mid')
    AMCPattern = re.compile('HDFC MF|DSP MF|Mirae MF|PGIM India MF|PPFAS MF|Quant MF')
    data = sheet2[sheet2['AMC Name'].str.contains(AMCPattern) & sheet2['CAP'].str.contains(CapPattern)]
    pivot = pd.pivot_table(data, index=['Invested In'], columns=['AMC Name'], values=['chng_val_asof_current_price'],
                           margins=True, margins_name="Grand Total", aggfunc=np.sum)
    pivot.columns = [str(s2) for (s1, s2) in pivot.columns.tolist()]
    pivot = pivot.reset_index()

    # FINAL TASK 3
    col = len(pivot.columns) - 1
    Slice = pivot.iloc[:, 1:col]
    Slice = Slice.replace(0, np.nan)
    arr = []
    for j in range(0, len(Slice)):
        x = Slice.loc[j].count()
        if (x >= 2):
            arr.append(round(j))
    Output = pivot.loc[arr]

    wb = xw.Book(file)
    sheet_oi_single = wb.sheets['Val_chng_Stock_perct']
    sheet_oi_single.clear()
    sheet_oi_single.range('A1').options(index=False).value = group
    # sheet_oi_single = wb.sheets['Val_chng_Stock_perct_amc']
    # sheet_oi_single.clear()
    # sheet_oi_single.range('A1').options(index=False).value = pivot
    sheet_oi_single = wb.sheets['Val_chng_Stock_more_than_2_val']
    sheet_oi_single.clear()
    sheet_oi_single.range('A1').options(index=False).value = Output
    wb.save()
    wb.close()


if __name__ == '__main__':
    mutual_fund()
    mutual_fund_analysis()
