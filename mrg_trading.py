import pandas as pd
import xlwings as xw
import dataframe_image as dfi
from utils import *
import requests

file = os.path.basename(__file__)
date_today = datetime.datetime.today() - datetime.timedelta(days=1)

date_today, prev_date, fut_date = get_market_prev_date_fut_date(date_today)
timesleep = datetime.datetime.now().strftime('%H:%M')


def mrg_trading(prev_date):
    ic(prev_date)
    print("https://archives.nseindia.com/content/equities/mrg_trading_" + prev_date.strftime('%d%m%y') + ".zip")
    response = requests.get(f"https://archives.nseindia.com/content/equities/mrg_trading_" +
                            prev_date.strftime('%d%m%y') + ".zip", timeout=1)
    open(RAW_DIR + '\\combineoi.zip', 'wb').write(response.content)
    zf = ZipFile(RAW_DIR + "\\combineoi.zip")
    zf.extractall(RAW_DIR)
    zf.close()

    if os.path.exists(RAW_DIR + '\\Margintrdg_' + prev_date.strftime('%d%m%y') + '\\mrg_trading_final_' +
                      prev_date.strftime('%d%m%Y') + '.csv'):
        mrg_trading = pd.read_csv(RAW_DIR + '\\Margintrdg_' + prev_date.strftime('%d%m%y') +
                                  '\\mrg_trading_final_' + prev_date.strftime('%d%m%Y') + '.csv', skiprows=10)
    elif os.path.exists(RAW_DIR + '\\mrg_trading_final_' + prev_date.strftime('%d%m%Y') + '.csv'):
        mrg_trading = pd.read_csv(RAW_DIR + '\\mrg_trading_final_' + prev_date.strftime('%d%m%Y') + '.csv'
                                  , skiprows=10)
    elif os.path.exists(RAW_DIR + '\\mrg_trading_' + prev_date.strftime('%d%m%Y') + '.csv'):
        mrg_trading = pd.read_csv(RAW_DIR + '\\mrg_trading_' + prev_date.strftime('%d%m%Y') + '.csv'
                                  , skiprows=10)
    elif os.path.exists(RAW_DIR + '\\mrg_trading_provisional_' + prev_date.strftime('%d%m%Y') + '.csv'):
        mrg_trading = pd.read_csv(RAW_DIR + '\\mrg_trading_provisional_' + prev_date.strftime('%d%m%Y') +
                                  '.csv', skiprows=10)

    mrg_trading['Date'] = prev_date
    mrg_trading['Date'] = pd.to_datetime(mrg_trading['Date'], format='%Y-%m-%d')

    # for duplicate stock values
    mrg_trading = mrg_trading.groupby(['Date', 'Symbol']).agg(
        {'Qty Fin by all the members(No.of Shares)': ['sum'],
         'Amt Fin by all the members(Rs. In Lakhs)': ['sum']}).reset_index()
    mrg_trading.columns = ['Date', 'Symbol', 'Qty_sum', 'Amt_sum']
    mrg_trading.drop(mrg_trading.tail(2).index, inplace=True)

    if not os.path.exists(PROCESSED_DIR + '\\mrg_trading_historical.csv'):
        mrg_trading['Date'] = mrg_trading['Date'].dt.strftime('%d-%m-%Y')
        mrg_trading.to_csv(PROCESSED_DIR + '\\mrg_trading_historical.csv', index=False)
    else:
        Historical = pd.read_csv(PROCESSED_DIR + '\\mrg_trading_historical.csv')
        Historical['Date'] = pd.to_datetime(Historical['Date'], format='%d-%m-%Y')
        if mrg_trading['Date'].max() != Historical['Date'].max():
            Historical = pd.concat([mrg_trading, Historical])
            Historical['Date'] = Historical['Date'].dt.strftime('%d-%m-%Y')
            Historical.to_csv(PROCESSED_DIR + '\\mrg_trading_historical.csv', index=False)

    dataframe = Historical[Historical['Qty_sum'] != 0]
    dataframe['Date'] = pd.to_datetime(dataframe['Date'], format='%d-%m-%Y')
    max_date = np.datetime64(dataframe['Date'].max())

    old_date = dataframe['Date'].unique()
    old_date = pd.DataFrame(old_date)
    old_date = old_date.rename(columns={0: "Date"})
    old_date.sort_values(by=['Date'], ascending=False, inplace=True)  # desc
    old_date['week_start'] = old_date['Date'].dt.to_period('W').apply(lambda r: r.start_time)
    week_start = old_date['week_start'].unique()
    week_start = pd.DataFrame(week_start)
    week_start = week_start.rename(columns={0: "Date"})
    old_monday = week_start['Date'][:7].to_list()

    max_date_df = dataframe[dataframe['Date'] == max_date]
    dataframe = dataframe[dataframe['Date'] != max_date]

    dataframe_10_days = pd.DataFrame()
    for mon in old_monday:
        temp = dataframe[dataframe['Date'] == mon]
        dataframe_10_days = pd.concat([dataframe_10_days, temp])

    dataframe_10_days = dataframe_10_days.pivot(index=['Symbol'], columns='Date', values=['Qty_sum'])
    dataframe_10_days.sort_values(by=['Date'], ascending=False, inplace=True, axis=1)
    dataframe_10_days.columns = dataframe_10_days.columns.droplevel()
    dataframe_10_days = pd.DataFrame(dataframe_10_days)

    # call average
    dataframe['avg_stock'] = dataframe['Symbol']
    dataframe['avg_date'] = dataframe['Date']
    dataframe['avg_col'] = dataframe['Qty_sum']
    dataframe.sort_values(by=['avg_date'], inplace=True)  # , ascending=False

    df_stock_avg_all = average(dataframe, prev_date)

    df_stock_avg_all = df_stock_avg_all.rename(columns={"avg_norm_mean": "Qty_norm_mean"})
    df_stock_avg_all['Date'] = pd.to_datetime(df_stock_avg_all['Date'], format='%d-%m-%Y')
    max_yesterday = np.datetime64(df_stock_avg_all['Date'].max())
    df_stock_avg_yest = df_stock_avg_all[df_stock_avg_all['Date'] == max_yesterday]
    df_stock_avg_yest.drop(['Qty_sum', 'Amt_sum', 'Date'], inplace=True, axis=1)
    df_stock_all = pd.merge(df_stock_avg_yest, max_date_df, on=['Symbol'])
    # calculations
    df_stock_all = df_stock_all[df_stock_all['Amt_sum'] >= 100]
    df_stock_all['sum/norm'] = df_stock_all['Qty_sum'] / df_stock_all['Qty_norm_mean']

    df_stock_all.sort_values(by=['sum/norm'], ascending=False, inplace=True)  # desc
    df_stock_all = df_stock_all.round({"sum/norm": 2})
    stock_metadata_df = pd.read_csv(METADATA_DIR + '\\StockMetadata.csv')[['Symbol', 'MarketCap', 'totalMarketCap']]
    df_stock_all = pd.merge(df_stock_all, stock_metadata_df, on=['Symbol'], how='left')
    df_stock_all['totalMarketCap'] = df_stock_all['totalMarketCap'].replace(0, 1)
    df_stock_all['totalMarketCap'] = df_stock_all['totalMarketCap'].astype(float)
    df_stock_all['amt/Total_m_cap'] = round((df_stock_all['Amt_sum'] / df_stock_all['totalMarketCap']) * 100, 2)
    df_stock_all.drop(['totalMarketCap'], inplace=True, axis=1)
    df_stock_all = df_stock_all[["Symbol", "MarketCap", "Qty_sum", "Amt_sum", "Qty_norm_mean",
                                 "sum/norm", "amt/Total_m_cap", "Date"]]
    df_stock_all.to_csv(METADATA_DIR + "\\get_avg_mrg_trading.csv", index=False)

    mrg_trading = pd.merge(df_stock_all, dataframe_10_days, how='left', left_on='Symbol', right_index=True)

    mrg_trading['p_chng'] = (mrg_trading[mrg_trading.columns[2]] - mrg_trading[mrg_trading.
                             columns[len(mrg_trading.columns) - 1]]) / mrg_trading[mrg_trading.columns[len(mrg_trading.
                                                                                                           columns) - 1]]
    mrg_trading = mrg_trading[mrg_trading['amt/Total_m_cap'] > 0.2]
    mrg_trading = mrg_trading[mrg_trading['p_chng'] > 0.2]

    df_stock_all = df_stock_all.sort_values(by=['amt/Total_m_cap'], ascending=False)
    mrg_trading_amt = pd.merge(df_stock_all, dataframe_10_days, how='left', left_on='Symbol', right_index=True)
    mrg_trading_amt['p_chng'] = (mrg_trading_amt[mrg_trading_amt.columns[2]] - mrg_trading_amt[
        mrg_trading_amt.columns[len(mrg_trading_amt.columns) - 1]]) / mrg_trading_amt[mrg_trading_amt.
        columns[len(mrg_trading_amt.columns) - 1]]
    mrg_trading_amt = mrg_trading_amt[mrg_trading_amt['amt/Total_m_cap'] > 0.2]
    mrg_trading_amt = mrg_trading_amt[mrg_trading_amt['p_chng'] > 0.2]

    days_10 = dataframe_10_days.columns
    data_excel_file = REPORT_DIR + "\\intermedent.xlsm"
    wb = xw.Book(data_excel_file)
    sheet_oi_single = wb.sheets('mrg_trading')
    sheet_oi_single.range("A1").options(index=None).value = mrg_trading
    mrg_trading['Qty_sum'] = mrg_trading['Qty_sum'].astype(int)
    mrg_trading['Amt_sum'] = mrg_trading['Amt_sum'].astype(float)
    mrg_trading['Qty_norm_mean'] = mrg_trading['Qty_norm_mean'].astype(int)
    mrg_trading['sum/norm'] = mrg_trading['sum/norm'].astype(float)
    mrg_trading['amt/Total_m_cap'] = mrg_trading['amt/Total_m_cap'].astype(float)
    mrg_trading = mrg_trading.head(50)
    format_dict = {'Amt_sum': '{:.2f}', 'sum/norm': '{:.2f}', 'amt/Total_m_cap': '{:.2f}',  'Date': '{:%Y-%m-%d}',
                   days_10[0]: '{:.0f}', days_10[1]: '{:.0f}', days_10[2]: '{:.0f}', days_10[3]: '{:.0f}',
                   days_10[4]: '{:.0f}', days_10[5]: '{:.0f}', 'p_chng': '{:.2f}'}
    mrg_trading = (mrg_trading.style.hide_index().format(format_dict)
                   .bar(color='#FFA07A', vmin=100_000, subset=['Qty_sum'], align='zero'))

    dfi.export(mrg_trading, SS_DIR + "\\mrg_trading.png")

    sheet_oi_single = wb.sheets('mrg_trading_amt')
    sheet_oi_single.range("A1").options(index=None).value = mrg_trading_amt

    mrg_trading_amt['Qty_sum'] = mrg_trading_amt['Qty_sum'].astype(int)
    mrg_trading_amt['Amt_sum'] = mrg_trading_amt['Amt_sum'].astype(float)
    mrg_trading_amt['Qty_norm_mean'] = mrg_trading_amt['Qty_norm_mean'].astype(int)
    mrg_trading_amt['sum/norm'] = mrg_trading_amt['sum/norm'].astype(float)
    mrg_trading_amt['amt/Total_m_cap'] = mrg_trading_amt['amt/Total_m_cap'].astype(float)
    mrg_trading_amt = mrg_trading_amt.head(50)
    mrg_trading_amt = (mrg_trading_amt.style.hide_index().format(format_dict)
                   .bar(color='#FFA07A', vmin=100_000, subset=['Qty_sum'], align='zero'))
    dfi.export(mrg_trading_amt, SS_DIR + "\\mrg_trading_amt.png")
    wb.save()
    app = xw.apps.active
    app.quit()


if __name__ == '__main__':

    try:
        mrg_trading(prev_date)
        execution_status('', file, logging.ERROR, date_today, 1)
    except Exception as e:
        print(e)
        execution_status(str(e), file, logging.ERROR, date_today, 0)
        # sendmail_err(file, e)
