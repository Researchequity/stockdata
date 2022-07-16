import pandas as pd
import datetime

DATE_TODAY = datetime.date.today() #- datetime.timedelta(days=1)
str_date = DATE_TODAY.strftime('%Y%m%d')
print(str_date)
filepath = r'/home/workspace/aggregate'


def call_cum():
    trade_his_df = pd.read_csv(filepath + '//tradeWatch_historical_' + str_date + '.csv', header=None)
    trade_his_df['row_count_cum'] = trade_his_df.groupby([1])[3].cumsum(axis=0)
    trade_his_df['sum_qty_cum'] = trade_his_df.groupby([1])[4].cumsum(axis=0)
    trade_his_df['nbuy_count_cum'] = trade_his_df.groupby([1])[5].cumsum(axis=0)
    trade_his_df['nsell_count_cum'] = trade_his_df.groupby([1])[6].cumsum(axis=0)
    trade_his_df['nbuy_qty_cum'] = trade_his_df.groupby([1])[8].cumsum(axis=0)
    trade_his_df['nsell_qty_cum'] = trade_his_df.groupby([1])[9].cumsum(axis=0)
    trade_his_df['cbuy_count_cum'] = trade_his_df.groupby([1])[10].cumsum(axis=0)
    trade_his_df['cbuy_qty_cum'] = trade_his_df.groupby([1])[11].cumsum(axis=0)
    trade_his_df['cbuy_value_cum'] = trade_his_df.groupby([1])[12].cumsum(axis=0)
    trade_his_df['csell_count_cum'] = trade_his_df.groupby([1])[13].cumsum(axis=0)
    trade_his_df['csell_qty_cum'] = trade_his_df.groupby([1])[14].cumsum(axis=0)
    trade_his_df['csell_value_cum'] = trade_his_df.groupby([1])[15].cumsum(axis=0)
    trade_his_df['traded_val'] = trade_his_df[4] * trade_his_df[7]
    trade_his_df['traded_val_cum'] = trade_his_df.groupby([1])['traded_val'].cumsum(axis=0)
    trade_his_df['vwap_cum'] = (trade_his_df['traded_val_cum']  / trade_his_df['sum_qty_cum']).round(0)
    trade_his_df = trade_his_df.rename(columns={0: "date ", 1: "token", 2: "stock", 3: "row_count", 4: "sum_qty", 5: "nbuy_count"
                                                , 6: "nsell_count", 7: "vwap_price", 8: "nbuy_qty", 9: "nsell_qty"
        , 10: "cbuy_count", 11: "cbuy_qty", 12: "cbuy_value", 13: "csell_count", 14: "csell_qty", 15: "csell_value"})
    trade_his_df.to_csv(r'/home/workspace/aggregate/report/cumilative.csv', index=None)


if __name__ == '__main__':
    call_cum()





