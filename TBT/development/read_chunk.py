import pandas as pd
from stream_num_trades_fut_filepath import *
from datetime import datetime

read_row = 0
open_price_read = 0
onetime = 0
DATE_TODAY = ''.join(str(datetime.today().date()).split('-'))
dumper_file = r'/home/workspace/dumper/fno_20220311_073001.csv_Clean'

# skip_row_path = os.path.join(SKIP_ROWS_NUM_DIRECTORY,
#                                  str(str(dumper_file).split('/')[-1]).split('.')[0]) + "_skipROW_{}.csv".format(DATE_TODAY)

# if os.path.exists(skip_row_path):
#     read_row = pd.read_csv(skip_row_path)['read_row']
#     read_row = int(read_row[0])
#
# if os.path.exists(skip_row_path):
#     read_row = pd.read_csv(skip_row_path)['read_row']
#     read_row = int(read_row[0])

print(read_row)
chunk = pd.read_csv(dumper_file, header=None, chunksize=6000000, skiprows=0)

for df_stream in chunk:
    df_stream[[3]].to_csv(r'/home/workspace/aggregate/tradewatch_fut/metadata/chunk1.csv', mode='a', index=False)


