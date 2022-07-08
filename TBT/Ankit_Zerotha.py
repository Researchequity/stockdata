from kiteconnect import KiteTicker, KiteConnect
import pandas as pd
from datetime import datetime, timedelta
import os

from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium import webdriver
import urllib.parse as urlparse
import time
import pyotp
import pywhatkit
import warnings



warnings.simplefilter(action="ignore", category=Warning)

pd.set_option('display.max_columns', None)
Filepath=r'\\192.168.41.190\program\stockdata\ORB'

# api_key = open('api_key.txt', 'r').read()
api_key = 'qplhtl8vlkk87651'

# api_secret = open('api_secret.txt', 'r').read()
api_secret = 'suzm8vk65gqr24k894tuabqlgc6t0vdj'

kite = KiteConnect(api_key=api_key)

access_token = open(Filepath+'\\'+'access_token.txt', 'r').read()
#access_token = 'cHwGeHsWqDTgCAghXo5Db6RZXoDztNbA'

kite.set_access_token(access_token)

symbol_list_arr = []
instruments = kite.instruments('NSE')
instruments = pd.DataFrame(instruments)

records = pd.read_csv(Filepath+'\\'+"todays_stocks.csv")
symbol = records['Symbol'].unique().tolist()

for t in symbol:
    symbol = instruments[instruments['tradingsymbol'] == t]
    tradingsymbol = symbol['tradingsymbol'].to_string(index=False)
    instrument_token = symbol['instrument_token'].to_string(index=False)
    symbol_list = [instrument_token, tradingsymbol]
    symbol_list_arr.append(symbol_list)

token_df = pd.DataFrame(symbol_list_arr, columns=['token_id', 'token_name'])
tokens = dict(zip(token_df['token_id'], token_df['token_name']))
print(tokens)


def Access_Token():
    if '09:20:00' <= datetime.strftime(datetime.now(), '%H:%M:%S') <= '09:29:00':
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--disable-notifications")
        driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=chrome_options)

        link = 'https://kite.trade/connect/login?api_key=qplhtl8vlkk87651&v=3'
        link2 = 'https://kite.zerodha.com/'

        driver.get(str(link))
        time.sleep(3)

        User_id = driver.find_element(By.XPATH, '//input[@id="userid"]')
        Passwd = driver.find_element(By.XPATH, '//input[@id="password"]')
        User_id.clear()
        Passwd.clear()
        User_id.send_keys("OKG047")
        Passwd.send_keys("Vijit9999")
        Button = driver.find_element(By.XPATH, '//button[@class="button-orange wide"]')
        Button.click()

        time.sleep(3)

        totp = pyotp.TOTP('T4O4IFTOZRNIIF4POW5JTNTYKSUZE5TC')
        totp = totp.now()
        topt_box = driver.find_element(By.XPATH, '//input[@id="totp"]')
        topt_box.clear()
        topt_box.send_keys(str(totp))
        Button.click()

        time.sleep(3)
        current_url = driver.current_url
        parse_url = urlparse.urlparse(current_url)
        request_token = urlparse.parse_qs(parse_url.query)['request_token'][0]
        print('request_token', request_token)
        driver.close()

        data = kite.generate_session(request_token, api_secret=api_secret)
        print(data['access_token'])
        kite.set_access_token(data['access_token'])
        with open('access_token.txt', 'w') as w:
            w.write(data['access_token'])
    else:
        print("Time Has been Passed to Run Access Token")


def square_off():
    if '15:00:00' <= datetime.strftime(datetime.now(), '%H:%M:%S') <= '15:20:00':
        data = pd.DataFrame(kite.orders())
        data = data[['order_id', 'status', 'order_timestamp', 'variety', 'exchange', 'tradingsymbol', 'instrument_token',
             'order_type', 'transaction_type', 'quantity', 'average_price']]
        data = data[~(data['status'] == 'REJECTED')]
        today_transaction=pd.read_csv(Filepath+'\\'+'Todays_Transaction.csv')

        data['order_id']=pd.to_numeric(data['order_id'], errors='ignore')
        today_transaction['order_id']=pd.to_numeric(today_transaction['order_id'], errors='ignore')

        duplicate = [value for value in today_transaction['order_id'].unique().tolist() if value in data['order_id'].unique().tolist()]
        print('duplicate Data')
        df1 = pd.DataFrame()
        for d in duplicate:
            a=data[data['order_id']==d]
            df1 = pd.concat([df1, a])

        print(df1)


        for token in tokens:
            df = df1[df1['tradingsymbol'] == tokens[token]]
            buy_complete_order = df['quantity'][df['status'].str.contains('COMPLETE') & df['transaction_type'].str.contains('BUY')]
            sell_complete_order = df['quantity'][df['status'].str.contains('COMPLETE') & df['transaction_type'].str.contains('SELL')]

            order_id = df['order_id'][(df['status'] == 'OPEN') | (df['status'] == 'TRIGGER PENDING')]

            if (len(order_id)==2):
                for co in order_id:
                    kite.cancel_order(variety='regular', order_id=str(co))
                    print('cancel order id is',co)


            print(sum(buy_complete_order), sum(sell_complete_order))

            if (sum(buy_complete_order) > sum(sell_complete_order)):
                remaining_sell = sum(buy_complete_order) - sum(sell_complete_order)
                print('For',tokens[token],"remaining  sell is  ", remaining_sell)

                order_id=order_id.values[0]

                ltp = kite.ltp('NSE' + ':' + tokens[token])
                ltp = pd.DataFrame(ltp)
                ltp=ltp['NSE' + ':' + tokens[token]].iloc[1]

                print("For ", tokens[token], " order id is ", str(order_id)," LTP : ",ltp)

                if '15:03:00' <= datetime.strftime(datetime.now(), '%H:%M:%S') <= '15:15:00':
                    print("Limit Order")

                    kite.modify_order(variety=kite.VARIETY_REGULAR,
                                      order_id=order_id,
                                      order_type=kite.ORDER_TYPE_LIMIT,
                                      price=ltp,
                                      validity=kite.VALIDITY_DAY)

                else:
                    print("Market Order")

                    kite.modify_order(variety=kite.VARIETY_REGULAR,
                                      order_id=order_id,
                                      order_type=kite.ORDER_TYPE_MARKET,
                                      validity=kite.VALIDITY_DAY)

            elif (sum(buy_complete_order) < sum(sell_complete_order)):
                remaining_buy = sum(sell_complete_order) - sum(buy_complete_order)
                print('For',tokens[token],"remaining buy is", remaining_buy)

                order_id = order_id.values[0]

                ltp = kite.ltp('NSE' + ':' + tokens[token])
                ltp = pd.DataFrame(ltp)
                ltp = ltp['NSE' + ':' + tokens[token]].iloc[1]

                print("For ", tokens[token], " order id is ", str(order_id), " LTP : ", ltp)

                if '15:03:00' <= datetime.strftime(datetime.now(), '%H:%M:%S') <= '15:15:00':
                    print("Limit order")

                    kite.modify_order(variety=kite.VARIETY_REGULAR,
                                      order_id=order_id,
                                      order_type=kite.ORDER_TYPE_LIMIT,
                                      price=ltp,
                                      validity=kite.VALIDITY_DAY)

                else:
                    print("Market order")

                    kite.modify_order(variety=kite.VARIETY_REGULAR,
                                      order_id=order_id,
                                      order_type=kite.ORDER_TYPE_MARKET,
                                      validity=kite.VALIDITY_DAY)


            else:
                print('i am out')
                pass

        data = pd.DataFrame(kite.orders())
        data = data[['order_id', 'status', 'order_timestamp', 'variety', 'exchange', 'tradingsymbol', 'instrument_token',
             'order_type', 'transaction_type', 'quantity', 'average_price']]

        today_transaction = pd.read_csv(Filepath+'\\'+'Todays_Transaction.csv')

        data['order_id'] = pd.to_numeric(data['order_id'], errors='ignore')
        today_transaction['order_id'] = pd.to_numeric(today_transaction['order_id'], errors='ignore')

        duplicate = [value for value in today_transaction['order_id'].unique().tolist() if
                     value in data['order_id'].unique().tolist()]
        print('duplicate Data')
        df1 = pd.DataFrame()
        for d in duplicate:
            a = data[data['order_id'] == d]
            df1 = pd.concat([df1, a])

        data = df1
        check = data['order_id'][(data['status'] == 'OPEN') | (data['status'] == 'TRIGGER PENDING')]

        if(len(check) == 0):
            data = data[(data['status'] == 'COMPLETE')]

            if not os.path.exists(Filepath+'\\'+'zerotha_Transaction.csv'):
                data.to_csv(Filepath+'\\'+'zerotha_Transaction.csv', index=False)
            else:
                Historical = pd.read_csv(Filepath+'\\'+'zerotha_Transaction.csv')
                #data['order_timestamp'] = data['order_timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
                duplicate = [value for value in data['order_id'].unique().tolist() if
                             value in Historical['order_id'].unique().tolist()]
                if bool(duplicate) == False:
                    Historical = pd.concat([data, Historical])
                    Historical.to_csv(Filepath+'\\'+'zerotha_Transaction.csv', index=False)
                    os.remove(Filepath+'\\'+'Todays_Transaction.csv')
                    #os.remove('todays_stocks.csv')
                else:
                    print("This Transaction Data are Already Exists in zerotha_Transaction")
    else:
        print("Time Has been Passed to Run Square-off Function")


def historical_data():
    if '09:30:00' <= datetime.strftime(datetime.now(), '%H:%M:%S') <= '09:45:00':
        order_df = pd.DataFrame(kite.orders(), columns=['tradingsymbol', 'status','transaction_type'])
        order_df = order_df[~(order_df['status'] == 'REJECTED')]
        order_df = order_df[~(order_df['status'] == 'CANCELLED')]

        #val_risk = int(input("Enter Value Risk For Tokens"))
        val_risk=1000

        for token in tokens:
            print('i am in ' + str(tokens[token]))
            if (tokens[token] not in order_df['tradingsymbol'].values):#not
                records=pd.read_csv(Filepath+'\\'+"todays_stocks.csv")
                records=records[records['Symbol'] == tokens[token]]

                high = records['15_Min_High'].values
                low = records['15_Min_Low'].values
                MarketCap=records['MarketCap'].values

                trigger_buy_val = round(0.05 * round((high[0] * 1.001) / 0.05), 2)
                trigger_sell_val = round(0.05 * round((low[0] * 0.999) / 0.05), 2)

                buy_val = round(0.05 * round((trigger_buy_val * 1.005) / 0.05), 2)
                sell_val = round(0.05 * round((trigger_sell_val * 0.995) / 0.05), 2)

                if(MarketCap[0]=='Mid'):
                    Qty = round((val_risk*5) / (trigger_buy_val - trigger_sell_val))
                else:
                    Qty = round(val_risk/ (trigger_buy_val - trigger_sell_val))


                ltp = kite.ltp('NSE' + ':' + tokens[token])
                ltp = pd.DataFrame(ltp)
                ltp=ltp['NSE' + ':' + tokens[token]].iloc[1]


                circuit_limit = pd.DataFrame(kite.quote(token))
                lower_circuit_limit = circuit_limit[str(token)].iloc[7]
                upper_circuit_limit = circuit_limit[str(token)].iloc[15]

                print(tokens[token], 'Market_cap:', MarketCap,',val_risk: ', val_risk,  ',High:', high[0], ',Low :', ',Qty :', Qty,
                      low[0],', trigger_buy_val : ',trigger_buy_val, ',buy_val :', buy_val,' , trigger_sell_val : ',trigger_sell_val, ',sell_val :', sell_val,',LTP :',ltp,
                      ', lower_circuit_limit : ',lower_circuit_limit,", upper_circuit_limit is : ", upper_circuit_limit)
                try:
                    if ((trigger_buy_val < upper_circuit_limit) & (trigger_sell_val > lower_circuit_limit) & (trigger_buy_val != trigger_sell_val) & (Qty > 1)):
                        if(trigger_buy_val > ltp):
                            print("SL Buy order")
                            buy_order = kite.place_order(variety=kite.VARIETY_REGULAR,
                                                         exchange=kite.EXCHANGE_NSE,
                                                         order_type=kite.ORDER_TYPE_SL,
                                                         tradingsymbol=tokens[token],
                                                         transaction_type=kite.TRANSACTION_TYPE_BUY,
                                                         quantity=Qty,
                                                         validity=kite.VALIDITY_DAY,
                                                         trigger_price=trigger_buy_val,
                                                         price=buy_val,
                                                         product=kite.PRODUCT_MIS)

                        else:
                            print("Limit buy order")
                            buy_order = kite.place_order(variety=kite.VARIETY_REGULAR,
                                                         exchange=kite.EXCHANGE_NSE,
                                                         order_type=kite.ORDER_TYPE_LIMIT,
                                                         tradingsymbol=tokens[token],
                                                         transaction_type=kite.TRANSACTION_TYPE_BUY,
                                                         quantity=Qty,
                                                         validity=kite.VALIDITY_DAY,
                                                         price=trigger_buy_val,
                                                         product=kite.PRODUCT_MIS)

                        if(trigger_sell_val < ltp):
                            print("SL Sell order")

                            sell_order = kite.place_order(variety=kite.VARIETY_REGULAR,
                                                          exchange=kite.EXCHANGE_NSE,
                                                          order_type=kite.ORDER_TYPE_SL,
                                                          tradingsymbol=tokens[token],
                                                          transaction_type=kite.TRANSACTION_TYPE_SELL,
                                                          quantity=Qty,
                                                          trigger_price=trigger_sell_val,
                                                          price=sell_val,
                                                          validity=kite.VALIDITY_DAY,
                                                          product=kite.PRODUCT_MIS)

                        else:
                            print("Limit Sell order")
                            sell_order = kite.place_order(variety=kite.VARIETY_REGULAR,
                                                          exchange=kite.EXCHANGE_NSE,
                                                          order_type=kite.ORDER_TYPE_LIMIT,
                                                          tradingsymbol=tokens[token],
                                                          transaction_type=kite.TRANSACTION_TYPE_SELL,
                                                          quantity=Qty,
                                                          price=trigger_sell_val,
                                                          validity=kite.VALIDITY_DAY,
                                                          product=kite.PRODUCT_MIS)


                        data = pd.DataFrame(kite.orders())
                        data = data[['order_id', 'status', 'order_timestamp', 'variety', 'exchange', 'tradingsymbol', 'instrument_token',
                             'order_type', 'transaction_type', 'quantity', 'average_price']]

                        lastest_trans=data.tail(2)

                        if not os.path.exists(Filepath+'\\'+'Todays_Transaction.csv'):
                            lastest_trans.to_csv(Filepath+'\\'+'Todays_Transaction.csv',index=False)
                        else:
                            lastest_trans.to_csv(Filepath+'\\'+'Todays_Transaction.csv',mode='a',index=False,header=False)

                        print(data)
                    else:
                        pass
                except:
                    pass


            else:
                print("This Order is Already Placed For " + tokens[token])
            #break
    else:
        print("Time Has been Passed to Run Historical Function")


#OKG047


first_time = 0
if first_time == 0:
    Access_Token()
    historical_data()
    square_off()

#pywhatkit.sendwhats_image('+917803058946', img, "Todays Stock", 30, True, 3)  # Ankit Sir:-+917803058946





"""
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--disable-notifications")
driver = webdriver.Chrome(ChromeDriverManager().install(),chrome_options=chrome_options)


link='https://kite.trade/connect/login?api_key=qplhtl8vlkk87651&v=3'
link2='https://kite.zerodha.com/'

driver.get(str(link))
time.sleep(3)

User_id=driver.find_element(By.XPATH,'//input[@id="userid"]')
Passwd=driver.find_element(By.XPATH,'//input[@id="password"]')

User_id.clear()
Passwd.clear()
User_id.send_keys("OKG047")
Passwd.send_keys("Vijit9999")

Button = driver.find_element(By.XPATH, '//button[@class="button-orange wide"]')
Button.click()

time.sleep(5)

totp = pyotp.TOTP('T4O4IFTOZRNIIF4POW5JTNTYKSUZE5TC')
totp=totp.now()

#//input[@id="totp"]
topt_box=driver.find_element(By.XPATH,'//input[@id="totp"]')
topt_box.clear()
topt_box.send_keys(str(totp))

Button.click()

time.sleep(2)
current_url = driver.current_url
parse_url = urlparse.urlparse(current_url)
request_token = urlparse.parse_qs(parse_url.query)['request_token'][0]

print('request_token',request_token)

data = kite.generate_session(request_token, api_secret=api_secret)
print('access_token',data['access_token'])
time.sleep(10)
driver.close()
#//input[@id="userid"]
#//input[@id="password"]

#//button[@class="button-orange wide"]

"""
















#Daily transaction file
"""
data = pd.DataFrame(kite.orders())
data = data[['order_id', 'status', 'order_timestamp', 'variety', 'exchange', 'tradingsymbol', 'instrument_token',
             'order_type', 'transaction_type', 'quantity', 'average_price']]
data = data[~(data['status'] == 'COMPLETE')]
print(data)
data.to_csv('zerotha_Transaction.csv',index=False,mode='a',header=False)

data.to_csv('Todays_Transaction.csv',index=False)
"""



#zerotha PNL
"""
    data = pd.DataFrame(kite.orders())
    data = data[['order_id', 'status', 'order_timestamp', 'variety', 'exchange', 'tradingsymbol', 'instrument_token',
                 'order_type', 'transaction_type', 'quantity', 'average_price']]
    df = data[(data['status'] == 'COMPLETE')]

    df['order_timestamp'] = pd.to_datetime(df['order_timestamp'], format='%Y-%m-%d %H:%M:%S')
    df['order_timestamp'] = df['order_timestamp'].dt.strftime('%d-%m-%Y')

    #df = df[df['order_timestamp'] == '29-06-2022']
    Symbols = df['tradingsymbol'].unique().tolist()

    df_all = []

    for symbol in Symbols:
        data = df[df['tradingsymbol'] == symbol]
        buydata = data[data['transaction_type'] == 'BUY']
        buydata['Buy_Value'] = round(buydata['average_price'] * buydata['quantity'], 2)
        buydata = buydata[['order_timestamp', 'tradingsymbol', 'quantity', 'Buy_Value']]

        selldata = data[data['transaction_type'] == 'SELL']
        selldata['Sell_Value'] = round(selldata['average_price'] * selldata['quantity'], 2)
        selldata = selldata[['order_timestamp', 'tradingsymbol', 'quantity', 'Sell_Value']]

        trans_data = pd.merge(buydata, selldata, on=['order_timestamp', 'tradingsymbol', 'quantity'], how='inner')
        trans_data.rename({'order_timestamp': 'Date', 'tradingsymbol': 'Symbol', 'quantity': 'Quantity'}, inplace=True,
                          axis=1)
        trans_data['Profit/Loss'] = trans_data['Sell_Value'] - trans_data['Buy_Value']
        trans_data['Profit/Loss %'] = (trans_data['Profit/Loss'] * 100) / trans_data['Buy_Value']
        trans_data['SL'] = 0
        df_all.append(trans_data)

    df_stock = pd.concat(df_all)

    df_stock.to_csv(r'\\192.168.41.190\program\stockdata\ORB\archive\zerotha_PNL.csv', mode='a', index=False,
                    header=False)
    print(df_stock)


"""