from datetime import datetime

date = ''.join(str(datetime.today().date()).split('-'))
input_file_path_cash="/home/dwh/tbt-ncash-str{}/" #+str(date)+"*.DAT "
input_file_path_fno="/home/dwh/tbt-fno-str{}/" #DUMP_"+str(date)+"*.DAT "
inc_file_path = "/home/workspace/dumper/metadata/stream_{}_"+str(date)+"_Inc.csv"  # incremental file
inc_file_path_fno = "/home/workspace/dumper/metadata/fno_inc_{}_"+str(date)+"_Inc.csv"  # incremental file
last_row_file_path="/home/workspace/dumper/metadata/stream_{}_"+str(date)+".DAT"# file to store last row number
last_row_file_path_fno="/home/workspace/dumper/metadata/fno_last_row_{}_"+str(date)+".DAT"# file to store last row number
clean_file_path = "/home/workspace/dumper/DUMP_" + str(date)  +"_07300{}"   +".csv_Clean"  # it is the path of file in windows server where we need to append data getting from server
get_stream_by_token_input_file = "/home/workspace/dumper/DUMP_" + str(date)  +"_07300{}"   +".csv_Clean"  # it is the path of file in windows server where we need to append data getting from server
clean_file_path_fno = "/home/workspace/dumper/fno_" + str(date)  +"_07300{}"   +".csv_Clean"  # it is the path of file in windows server where we need to append data getting from server
dumper_file_dir= "/home/workspace/dumper/"
aggregate_file_dir= "/home/workspace/aggregate/"
python_ankit_dir = r'/home/workspace/production/python_ankit'
trade_watch_dir = r'/home/workspace/aggregate/tradewatch'