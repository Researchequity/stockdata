import os
import sys
import time
import logging,threading
import pandas as pd
from glob import glob
from datetime import datetime
import datetime as dtm
#import xlwings as xw
import getpass

from stream_num_trades_filepath import *

import warnings
warnings.simplefilter(action="ignore",category=Warning)

#Input from Excel
# if getpass.getuser() == 'ankit':
#     print('')
# else:
#     data_excel_file = "E:\\DUMPER\\tradewatch\\not_sure.xlsx"
#     #wb = xw.Book(data_excel_file)
#     #exit()
                            
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)

formatLOG = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

def LOG_insert(file_name, format, text, level):
    file=os.path.join(LOG_FILE_DIRECTORY,file_name)
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

#def return_factor_buy(element):
#    return element.agg({6:sum}).values[0]/element.agg({7:sum}).values[0]


#last min weight average
#def return_ratio_final(name):
#    name['product']=(name['sum_qty']*name['vwap_price'])
#    return name.agg({'product':sum}).values[0]//(name.agg({'sum_qty':sum}).values[0])

def return_result_by_odr_T(df_stream,token):
    #import pdb;pdb.set_trace() 
    filtered_df_stream = df_stream[df_stream[1] == 'T']
    filtered_df_stream[9]=filtered_df_stream[5]*filtered_df_stream[6]
    frame = filtered_df_stream.groupby([7,4]).agg({0:['count'],6:['sum'],9: ['sum']}).reset_index()
    frame.columns = ['Date','token','row_count', 'sum_qty','9_sum']
    frame['vwap_price']= frame['9_sum']//frame['sum_qty']
    frame['token']=frame['token'].astype(int)
    result=pd.merge(frame,token,on='token')
    return result

def row_count_N_and_B(stream,token):
    filtered_df_stream = stream[(stream[1]=='N') & (stream[4]=='B')]
    result = filtered_df_stream.groupby([7,3]).agg({0:['count'],6:['sum']}).reset_index()
    result.columns = ['Date','token','nbuy_count', 'nbuy_qty']
    result = pd.merge(result, token, on='token')
    return result


def row_count_N_and_S(stream,token):
    filtered_df_stream = stream[(stream[1]=='N') & (stream[4]=='S')]
    result = filtered_df_stream.groupby([7,3]).agg({0:['count'],6:['sum']}).reset_index()
    result.columns = ['Date','token','nsell_count', 'nsell_qty']
    result = pd.merge(result, token, on='token')
    return result



def start_thread(df_norman_trd,interval, token,dumper_file,LOG_FILE_NAME):

    print("**************************** thread  started ***************************")
    
    resulting_filepath=os.path.join(TRADE_WATCH,"tradeWatch_historical_{}.csv".format(DATE_TODAY))
    skip_row_path=os.path.join(SKIP_ROWS_NUM_DIRECTORY,str(str(dumper_file).split('/')[-1]).split('.')[0])+"_skipROW_{}.csv".format(DATE_TODAY)
    LAST_MIN_DATA_FILE=os.path.join(LAST_MIN_DATA,"lastmindata_{}.csv".format(DATE_TODAY))
    OPENING_DATA_FILE=os.path.join(OPENING_DATA,"openingdata_{}.csv".format(DATE_TODAY))
    
    #skip_row_path=os.path.join(SKIP_ROWS_NUM_DIRECTORY,str(str(dumper_file).split('/')[-1]).split('.')[0])+".csv"
    #print(skip_row_path,"skip row path")

    header = True

    read_row = 0
    if os.path.exists(skip_row_path):
        read_row = pd.read_csv(skip_row_path)['read_row']
        read_row = int(read_row[0])


    if os.path.exists(LAST_MIN_DATA_FILE):
        last_min_df=pd.read_csv(LAST_MIN_DATA_FILE)
    else:
        last_min_df=pd.DataFrame()

    while datetime.now() <= datetime(year=datetime.today().year, day=datetime.today().day, month=datetime.today().month, hour=23, minute=59, second=00):
        #import pdb;pdb.set_trace()

        #if not header:
        if os.path.exists(skip_row_path):
            read_row = pd.read_csv(skip_row_path)['read_row']
            read_row = int(read_row[0])
        try:

            chunk = pd.read_csv(dumper_file,header=None,chunksize=1000000,skiprows=read_row)
            

            for df_stream in chunk:
                
                try:
                    df_stream[7] = pd.to_datetime(df_stream[7]).dt.floor("{}".format("1min" if interval == 1 else "5min"))
                    
                except Exception as e:
                    LOG_insert(LOG_FILE_NAME, formatLOG, str(e) + " error in 2", logging.INFO)

                last_min = pd.to_datetime(df_stream[7].max())
                
            

                if len(df_stream):

                    try:
                        df_1 = return_result_by_odr_T(df_stream, token)

                        df_2 = row_count_N_and_B(df_stream,token)

                        df_3 = row_count_N_and_S(df_stream,token)

                        df_4 = pd.merge(df_1, df_2, on=['Date', 'token', 'Symbol'])

                        df_5 = pd.merge(df_4, df_3, on=['Date', 'token', 'Symbol'])
                    except Exception as e:
                        LOG_insert(LOG_FILE_NAME, formatLOG, str(e) + " error in 3", logging.INFO)

                    read_row=read_row+len(df_stream)
                    pd.DataFrame([{'read_row': read_row}]).to_csv(skip_row_path, index=False)
                    
                    if len(last_min_df):
                        df_5= pd.concat([df_5,last_min_df])
                        last_min_df=pd.DataFrame()
                        

                    last_min_df=df_5[(df_5['Date'] == last_min)]        

                    TODAY_DAY= last_min.day
                    TODAY_MONTH= last_min.month
                    TODAY_YEAR= last_min.year
                    today_09_15 = datetime(day=TODAY_DAY,month=TODAY_MONTH,year=TODAY_YEAR,hour=9,minute=15,second=0)
                    open_df = df_5[(df_5['Date'] == today_09_15 )]

                    open_df[['token','vwap_price']].to_csv(OPENING_DATA_FILE,mode='a',header=None,index=False)
                    
                    if len(last_min_df):
                        last_min_df.to_csv(LAST_MIN_DATA_FILE)

                    df_5=df_5[(df_5['Date'] < last_min)]

                    if len(df_5): 


                        df_5['vwap_price'] = df_5['vwap_price'].astype(int)
                        df_5_groupby = df_5.groupby(['Date', 'token', 'Symbol']).agg({'row_count': ['sum'], 'sum_qty': ['sum'], 'nbuy_count': ['sum'],'nsell_count':
['sum'],'vwap_price':['mean'], 'nbuy_qty': ['sum'], 'nsell_qty': ['sum'] })


                        df_5_groupby.reset_index(inplace=True)
                        df_5_groupby.columns = ["".join(x) for x in df_5_groupby.columns.ravel()]
                        df_5_groupby.rename(columns={"row_countsum":"row_count","sum_qtysum":"sum_qty","nbuy_countsum":"nbuy_count","nsell_countsum":"nsell_count","vwap_pricemean":"vwap_price","nbuy_qtysum":"nbuy_qty","nsell_qtysum":"nsell_qty"}, inplace = True)
                        df_5_groupby['vwap_price'] = df_5_groupby['vwap_price'].astype(int)

                        df_5_groupby.to_csv(resulting_filepath, mode='a', index=False, header=False) # keep same


                        

                        try:
                            df_norman_trd.rename(columns={'Stock': 'Symbol'}, inplace=True)

                            df_6 = pd.merge(df_5_groupby, df_norman_trd, on='Symbol')
                            df_6 = df_6[["Date", "token", "Symbol","row_count","sum_qty","nbuy_count","nsell_count","vwap_price","normean_quantity","normean_trd","nbuy_qty","nsell_qty"]]

                            df_6 = df_6[((df_6['sum_qty'] >= 10*df_6['normean_quantity']) & (df_6['normean_trd'] >10)) | (df_6['sum_qty'] >= 100*df_6['normean_quantity'])]


                            df_6.to_csv(os.path.join(TRADE_WATCH,"trade_result_final_{}.csv".format(DATE_TODAY)),mode='a',index=False, header=False)
                            header = False

                            #import pdb;pdb.set_trace()
                            df_7= pd.DataFrame()
                            df_7=pd.read_csv(os.path.join(TRADE_WATCH,"trade_result_final_{}.csv".format(DATE_TODAY)),header=None)


                            df_8 = pd.DataFrame()
                            
                            df_8=df_7.groupby([1])
                            df_8 = df_8[[5]].count()
                            df_8.rename(columns={5:"repeat"}, inplace = True)
                            df_8.index.name = 'token'
                            df_8.reset_index(inplace=True)
                            
                            
                            #df_8['factor_buy'] = df_7.groupby([1]).apply(return_factor_buy)
                            
                            
                            df_6 = pd.merge(df_6, df_8, on=['token'])
                            
                            df_6['r_qty'] = df_6['sum_qty']//df_6['normean_quantity']
                            df_6['r_trade'] = df_6['row_count']//df_6['normean_trd']
                            df_6['BUY'] = df_6['nbuy_qty']//df_6['nsell_qty']
                            df_6.dropna(subset = ['Symbol'], inplace=True)
                            
                            open_data=pd.read_csv(OPENING_DATA_FILE,header=None)
                            
                            open_data.rename(columns={0:'token',1:'o_price'},inplace=True)
                            
                            df_6 = pd.merge(df_6,open_data,on=['token'])
                            
                            #df_6.to_csv(os.path.join(TRADE_WATCH,"df7_{}.csv".format(DATE_TODAY)),index=False, header=False)
                            
                            #check num of traded in X min should be 30 , and
                            df_6 = df_6[(df_6['repeat'] >= 0) & (df_6['row_count'] >= 20) & (df_6['vwap_price'] > df_6['o_price']) & (df_6['normean_trd'] >10)] 
                            
                            if len(df_6):
                                #wb.sheets("Current").range("A1").options(index=False).value = df_6
                                #'new_buyer','new_seller'
                                #print(df_6[['Date','token','normean_quantity','sum_qty','normean_trd','row_count','vwap_price', 'o_price','BUY','repeat','r_qty', 'Symbol']])

                                df_6[['Date','token','normean_quantity','sum_qty','normean_trd','row_count','vwap_price', 'o_price','BUY','repeat','r_qty', 'Symbol']].to_csv(os.path.join(TRADE_WATCH,"stream_op_{}.csv".format(DATE_TODAY)),mode='a',index=False, header=False)
                            

                                
                        except Exception as e:
                            print(e)
                            LOG_insert(LOG_FILE_NAME, formatLOG, str(e)+"error in df_6 or 7", logging.INFO)
                                             
                    else:
                        pass

                  

                else:
                    LOG_insert(LOG_FILE_NAME, formatLOG, "Please wait for Data", logging.INFO)
                

            
        except Exception as e:
            if last_min == datetime(year=datetime.today().year, day=datetime.today().day, month=datetime.today().month, hour=15, minute=29, second=00) and datetime.now() >= datetime(year=datetime.today().year, day=datetime.today().day, month=datetime.today().month, hour=15, minute=31, second=00):
                df_5= last_min_df
                df_5['vwap_price'] = df_5['vwap_price'].astype(int)
                df_5_groupby = df_5.groupby(['Date', 'token', 'Symbol']).agg({'row_count': ['sum'], 'sum_qty': ['sum'], 'nbuy_count': ['sum'],'nsell_count':['sum'],'vwap_price':['mean'], 'nbuy_qty': ['sum'], 'nsell_qty': ['sum'] })
                df_5_groupby.reset_index(inplace=True)
                df_5_groupby.columns = ["".join(x) for x in df_5_groupby.columns.ravel()]
                df_5_groupby.rename(columns={"row_countsum":"row_count","sum_qtysum":"sum_qty","nbuy_countsum":"nbuy_count","nsell_countsum":"nsell_count","vwap_pricemean":"vwap_price","nbuy_qtysum":"nbuy_qty","nsell_qtysum":"nsell_qty"}, inplace = True)
                df_5_groupby['vwap_price'] = df_5_groupby['vwap_price'].astype(int)
                df_5_groupby.to_csv(resulting_filepath, mode='a', index=False, header=False) # keep same
                last_min_df=pd.DataFrame()                
                last_min_df.to_csv(LAST_MIN_DATA_FILE)
                break
            
            LOG_insert(LOG_FILE_NAME, formatLOG, str(e)+"file is over", logging.ERROR)
            time.sleep(interval*58)
                    

def filtering_stream_data():

    interval=1 #int(input("Enter Time Interval in min eg. 1 0r 5: "))


    if interval in [1,5]:

        division_factor= 75 if int(interval)==5 else 375

        df_norman_trd=pd.read_csv(NORMEN_TRD_FILE)
        df_norman_trd['normean_quantity']=df_norman_trd['mean_quantity'].apply(lambda x: (x//division_factor))
        df_norman_trd['normean_trd']=df_norman_trd['mean_trd'].apply(lambda x: (x//division_factor))
        df_norman_trd = df_norman_trd[['Stock','normean_quantity','normean_trd']]
                
        token = pd.read_csv(TOKEN_SECURITY_FILE)

        files = glob(os.path.join(DUMPER_FILE_DIRECTORY, 'DUMP_*.csv_Clean'))
        print(files)
        for dumper_file in files:

            if str(str(dumper_file).split('/')[-1]).split('_')[1]==DATE_TODAY:

                #if str(str(dumper_file).split('/')[-1]).split('_')[1]==DATE_TODAY or 1==1:
                thread = threading.Thread(target=start_thread, args=(df_norman_trd, interval, token,dumper_file,LOG_FILE_NAME))
                thread.start()
            else:
                print("File for date {} not found".format(DATE_TODAY))
                LOG_insert(LOG_FILE_NAME, formatLOG, "File for date {} not found".format(DATE_TODAY), logging.INFO)
                

    else:
        LOG_insert(LOG_FILE_NAME, formatLOG, "time interval provided is not 1 minute or 5 minute", logging.INFO)

LOG_FILE_NAME=str(str(os.path.abspath(__file__)).split('/')[-1]).split('.')[0]+".log"
#LOG_FILE_NAME=str(str(os.path.abspath(__file__)).split('/')[-1]).split('.')[0]+".log"
DATE_TODAY = ''.join(str(datetime.today().date()).split('-'))
TODAY_DAY= datetime.today().day
TODAY_MONTH= datetime.today().month
TODAY_YEAR= datetime.today().year


try:
    filtering_stream_data()
except Exception as e:
    print(e)
    LOG_insert(LOG_FILE_NAME, formatLOG, e, logging.ERROR)
    

