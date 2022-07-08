import sys
import threading,io,time
from datetime import datetime
#from .get_stock_metadata import HOLIDAY_FILE_PATH
import dateutil
import paramiko
import logging
import pandas as pd
import datetime as dt
import subprocess
from os import path
from glob import glob
import os
from filepath import input_file_path_cash, inc_file_path, last_row_file_path, clean_file_path

formatLOG = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

def LOG_insert(file, format, text, level):
    infoLog = logging.FileHandler(file)
    infoLog.setFormatter(format)
    logger = logging.getLogger(file)
    logger.setLevel(level)

    if not logger.handlers:
        logger.addHandler(infoLog)
        if (level == logging.INFO):
            logger.info(text)
        if (level == logging.ERROR):
            logger.error(text)
        if (level == logging.WARNING):
            logger.warning(text)

    infoLog.close()
    logger.removeHandler(infoLog)

    return


# Opening a file does NOT implicitly read nor load its contents. Even when you do so using Python's context management protocol (the with keyword)

def holiday_check(path):
    try:
        hol_df = pd.read_csv(path)
        holiday_list = [dateutil.parser.parse(dat).date() for dat in hol_df['Holiday'].values]
        "Weekday Return day of the week, where Monday == 0 ... Sunday == 6."
        date_today = datetime.today().date()
        if date_today not in holiday_list and date_today.weekday() != 6 and date_today.weekday() != 5:
            return True
        else:return False
    except Exception as e:
        LOG_insert("/home/workspace/dumper/file.log", formatLOG, e, logging.INFO)

def del_last_day_file(i):
    try:
        i = i+1
        past_date = (datetime.today() - dt.timedelta(days=1)).date()
        past_date_str = ''.join(str(past_date).split('-'))
        inc_file_path="/home/workspace/dumper/metadata/stream_{}_{}_Inc.csv".format(str(i), past_date_str)
        last_row_file_path="/home/workspace/dumper/metadata/stream_{}_{}.DAT".format(str(i), past_date_str)
        os.system('rm -f {}'.format(inc_file_path))
        os.system('rm -f {}'.format(last_row_file_path))

    except Exception as e:
        LOG_insert("/home/workspace/dumper/file.log", formatLOG, e, logging.INFO)
        return None

def download_stream(index,input_path,inc_file_path,last_row_file_path,break_time,clean_file_path):
    
    print(last_row_file_path)
    
    read_row = 0
    onetime = 0
    while datetime.now()<=break_time or onetime == 0: # loop will break when time at server exceed 3:31:00 (10:01:00 at server) PM
        onetime = 1
        try:

            if os.path.exists(last_row_file_path):
                read_row = pd.read_csv(last_row_file_path)['read_row']
                read_row = int(read_row[0])

            chunk = pd.read_csv(input_path , header  = None, chunksize=50000,skiprows=read_row) #nrows=100000)

            for df_stream_token in chunk:
                read_row=read_row+len(df_stream_token)
                
                # drop columns not needed
                df_stream_token['datetime'] = pd.to_datetime('1980-01-01 00:00:00') + pd.to_timedelta(df_stream_token[6]).dt.floor("S")
                df_stream_token = df_stream_token.drop([0, 1, 2, 3,6], axis=1)
                df_stream_token = df_stream_token[df_stream_token[5]!="M"]
                df_stream_token.to_csv(clean_file_path , index=False, mode='a', header=False)
                
                pd.DataFrame([{'read_row': read_row}]).to_csv(last_row_file_path, index=False)
                print(read_row)



        except Exception as e:
            # if there is no file at server or any other issue
            LOG_insert("/home/workspace/dumper/file.log", formatLOG, e, logging.INFO)
            #logging.info(str(e))



def start_stream_download():
    try:
        # establishing connection with server
        #ssh=get_connected()

        # removing hyphon (-) from date and making it continuous string for appending it in path
        date = ''.join(str(datetime.today().date()).split('-'))

        holiday_file_path="C:\\home\\holiday.csv"
        #if 0==0: #holiday_check(holiday_file_path):

        for j in range(4):
            i = j + 1
            date = ''.join(str(datetime.today().date()).split('-'))
            files = glob(os.path.join(input_file_path_cash.format(i), 'DUMP_{}*.DAT'.format(date)))
            print(files)

            for input_file in files:
                break_time=datetime(year=datetime.today().year,day=datetime.today().day,month=datetime.today().month,hour=15,minute=35,second=00)
                del_last_day_file(i)
                print('thread start')
                thread=threading.Thread(target=download_stream,args=(j,input_file,inc_file_path.format(i),last_row_file_path.format(i),break_time,clean_file_path.format(i)))
                thread.start() #starting thread for each stream, here we have 4 stream

    except Exception as e:
        LOG_insert("/home/workspace/dumper/file.log", formatLOG, e, logging.INFO)


try:

    start_stream_download()
except Exception as e:
    LOG_insert("/home/workspace/dumper/file.log", formatLOG, e, logging.INFO)
