import xlwings as xw
from utils import *


def read_csv():
    trade_result_pre_open = pd.read_csv(PROCESSED_DIR + '\\22_files\\trade_result_pre_open.csv')
    trade_result_pre_open_nifty = pd.read_csv(PROCESSED_DIR + '\\22_files\\trade_result_pre_open_nifty.csv')
    trade_result_pre_open_banknifty = pd.read_csv(PROCESSED_DIR + '\\22_files\\trade_result_pre_open_banknifty.csv')
    trade_watch_merge_Report_Buy = pd.read_csv(PROCESSED_DIR + '\\22_files\\trade_watch_merge_Report_Buy.csv')
    trade_watch_merge_Report_Sell = pd.read_csv(PROCESSED_DIR + '\\22_files\\trade_watch_merge_Report_Sell.csv')
    trade_watch_merge_fut_buy = pd.read_csv(PROCESSED_DIR + '\\22_files\\trade_watch_merge_fut_buy.csv')
    trade_watch_merge_fut_sell = pd.read_csv(PROCESSED_DIR + '\\22_files\\trade_watch_merge_fut_sell.csv')
    buy_last_30min = pd.read_csv(PROCESSED_DIR + '\\22_files\\watch_last_30_min_perm_trades_buy_last_30min.csv')
    BTST_buy = pd.read_csv(PROCESSED_DIR + '\\22_files\\watch_BTST_perm_BTST_buy.csv')
    max_prog = pd.read_csv(PROCESSED_DIR + '\\22_files\\max_progression.csv')
    SL_buy = pd.read_csv(PROCESSED_DIR + '\\22_files\\SL_Daily_SL_buy.csv')
    SL_sell = pd.read_csv(PROCESSED_DIR + '\\22_files\\SL_Daily_SL_sell.csv')
    Previous = pd.read_csv(PROCESSED_DIR + '\\22_files\\trade_result_Previous.csv')
    Raw_Data = pd.read_csv(PROCESSED_DIR + '\\22_files\\trade_result_Raw_Data.csv')

    # read excel and dump data
    data_excel_file = REPORT_DIR + "\\Reporting_22_files.xlsm"
    wb = xw.Book(data_excel_file)
    sheet_oi_single = wb.sheets('pre_open')
    sheet_oi_single.range("A1").options(index=None).value = trade_result_pre_open
    sheet_oi_single = wb.sheets('pre_open_nifty')
    sheet_oi_single.range("A1").options(index=None).value = trade_result_pre_open_nifty
    sheet_oi_single = wb.sheets('pre_open_banknifty')
    sheet_oi_single.range("A1").options(index=None).value = trade_result_pre_open_banknifty
    sheet_oi_single = wb.sheets('pre_open_banknifty')
    sheet_oi_single.range("A1").options(index=None).value = trade_result_pre_open_banknifty
    sheet_oi_single = wb.sheets('Report_buy')
    sheet_oi_single.clear()
    sheet_oi_single.range("A1").options(index=None).value = trade_watch_merge_Report_Buy
    sheet_oi_single = wb.sheets('Report_Sell')
    sheet_oi_single.clear()
    sheet_oi_single.range("A1").options(index=None).value = trade_watch_merge_Report_Sell
    sheet_oi_single = wb.sheets('fut_buy')
    sheet_oi_single.clear()
    sheet_oi_single.range("A1").options(index=None).value = trade_watch_merge_fut_buy
    sheet_oi_single = wb.sheets('fut_sell')
    sheet_oi_single.clear()
    sheet_oi_single.range("A1").options(index=None).value = trade_watch_merge_fut_sell
    sheet_oi_single = wb.sheets('buy_last_30min')
    sheet_oi_single.clear()
    sheet_oi_single.range("A1").options(index=None).value = buy_last_30min
    sheet_oi_single = wb.sheets('BTST_buy')
    sheet_oi_single.clear()
    sheet_oi_single.range("A1").options(index=None).value = BTST_buy
    sheet_oi_single = wb.sheets('max_prog')
    sheet_oi_single.clear()
    sheet_oi_single.range("A1").options(index=None).value = max_prog
    sheet_oi_single = wb.sheets('SL_buy')
    sheet_oi_single.clear()
    sheet_oi_single.range("A1").options(index=None).value = SL_buy
    sheet_oi_single = wb.sheets('SL_sell')
    sheet_oi_single.clear()
    sheet_oi_single.range("A1").options(index=None).value = SL_sell
    sheet_oi_single = wb.sheets('Previous')
    sheet_oi_single.clear()
    sheet_oi_single.range("A1").options(index=None).value = Previous
    sheet_oi_single = wb.sheets('Raw_Data')
    sheet_oi_single.clear()
    sheet_oi_single.range("A1").options(index=None).value = Raw_Data
    wb.save()


if __name__ == '__main__':
    read_csv()