import time
from datetime import datetime
import pandas as pd
from glob import glob
import os
from filepath import *

now = datetime.now()
current_time = now.strftime("%H:%M")
date = ''.join(str(datetime.today().date()).split('-'))

last_min = pd.to_datetime(datetime.today().date())
TODAY_DAY = last_min.day
TODAY_MONTH = last_min.month
TODAY_YEAR = last_min.year


def sl_scan():
    for i in range(4):
        read_path = dumper_file_dir + "DUMP_" + str(date) + "_" + "07300" + str(i + 1) + ".csv_Clean"
        df_pre_open = pd.read_csv(read_path, header=None, nrows=1000000)
        df_pre_open[7] = pd.to_datetime(df_pre_open[7])
        df_pre_open = df_pre_open[
            df_pre_open[7] < datetime(day=TODAY_DAY, month=TODAY_MONTH, year=TODAY_YEAR, hour=9, minute=13, second=0)]
        max_row_pre_open = df_pre_open[0].max().item()
        df_pre_open = []

        # read_path = "F:\\Archive_Clean\\" + date +"\\DUMP_" + date +"_" +  "07300" + str(i+1) + ".csv_Clean"
        print(i + 1)

        dest_path = aggregate_file_dir + "sl/scan/sl_" + str(date) + "_" + "07300" + str(i + 1) + ".csv"

        # Error orders
        min_value = int('0')

        with open(dest_path, 'w') as fd:
            with open(read_path, 'r') as read_obj:
                for line in read_obj:
                    first = line.split(',')
                    row_num = int(first[0])
                    if (first[
                            1] == "N") and row_num > max_row_pre_open:  # pd.to_datetime(first[7]) >  datetime(day=TODAY_DAY,month=TODAY_MONTH,year=TODAY_YEAR,hour=9,minute=13,second=0):
                        if (int(first[2]) >= int(min_value)):

                            min_value = first[2]
                        else:
                            fd.write(line)


def sl_mean():
    date_file = []
    for file in glob(aggregate_file_dir + 'sl/scan/sl_*.csv'):
        date_file.append(file[37:45])

    df = pd.DataFrame()
    df['date'] = date_file
    df_date = pd.DataFrame()

    for date in df.date.unique():

        try:
            SL_all_df = pd.DataFrame()
            print(date)
            file1 = os.path.join(aggregate_file_dir + "sl/scan/sl_{}_073001.csv".format(date))
            SL_df = pd.read_csv(file1, header=None)
            SL_all_df = pd.concat([SL_all_df, SL_df])

            file2 = os.path.join(aggregate_file_dir + "sl/scan/sl_{}_073002.csv".format(date))
            SL_df = pd.read_csv(file2, header=None)
            SL_all_df = pd.concat([SL_all_df, SL_df])

            file3 = os.path.join(aggregate_file_dir + "sl/scan/sl_{}_073003.csv".format(date))
            SL_df = pd.read_csv(file3, header=None)
            SL_all_df = pd.concat([SL_all_df, SL_df])

            file4 = os.path.join(aggregate_file_dir + "sl/scan/sl_{}_073004.csv".format(date))
            SL_df = pd.read_csv(file4, header=None)
            SL_all_df = pd.concat([SL_all_df, SL_df])

            result = SL_all_df.groupby([3, 4]).agg({0: ['count'], 6: ['sum']}).reset_index()
            result.columns = ['token', 'OrderType', 'nSL_count', 'nSL_qty']
            pivot_temp_df = result.pivot(index=['token'], columns='OrderType',
                                         values=['nSL_count', 'nSL_qty']).reset_index()
            pivot_temp_df.columns = ['token', 'nBuySL_count', 'nSellSL_count', 'nBuySL_qty', 'nSellSL_qty']
            pivot_temp_df = pivot_temp_df.round(0)

            pivot_temp_df.to_csv(aggregate_file_dir + "sl/average/slaverage" + date + ".csv", index=False)
        except Exception as e:
            print('file not found for' + date)

    # Get Average from daily aggregate

    SL_avg_df = pd.DataFrame()
    for file in glob(aggregate_file_dir + 'sl/average/slaverage*.csv'):
        print(file)
        SL_df = pd.read_csv(file)
        SL_avg_df = pd.concat([SL_avg_df, SL_df])

    result = SL_avg_df.groupby(['token']).agg(
        {'nBuySL_count': ['mean'], 'nSellSL_count': ['mean'], 'nBuySL_qty': ['mean'],
         'nSellSL_qty': ['mean']}).reset_index()
    result.columns = ['token', 'nBuySL_count_mean', 'nSellSL_count_mean', 'nBuySL_qty_mean', 'nSellSL_qty_mean']
    result = result.round(0)

    if current_time > "15:30":
        result.to_csv(aggregate_file_dir + "sl/slaverage.csv", index=False)


while True and current_time < "22:00":
    sl_scan()
    sl_mean()
    time.sleep(900)
    now = datetime.now()
    current_time = now.strftime("%H:%M")
