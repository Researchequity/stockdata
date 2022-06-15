import pandas as pd
import datetime
from datetime import timedelta
import smtplib
from email.message import EmailMessage
import dataframe_image as dfi
import mimetypes
from email.utils import make_msgid
import re

PROCESSED_DIR = r'\\192.168.41.190\program\stockdata\processed'
METADATA_DIR = r'\\192.168.41.190\program\stockdata\metadata'
SS_DIR = r'\\192.168.41.190\program\stockdata\screenshot'
All_date = []
Missing_Date = []
TODAY_DATE = datetime.date.today()


def sendmail(file):
    EMAIL_ADDRESS = 'researchequity10@gmail.com'
    EMAIL_PASSWORD = 'noxtajxjdmzdjlkb'
    msg = EmailMessage()
    msg['Subject'] = 'missing dates as of:' + str(file) + str(TODAY_DATE)
    msg['From'] = EMAIL_ADDRESS
    msg['To'] = ['rohnkoria@gmail.com', 'engineers1030164@gmail.com','kinjalchauhan1999@gmail.com']
    image_cid = make_msgid()
    msg.add_attachment("""\
    <!DOCTYPE html>
    <html>
        <body>
            <img src="cid:{image_cid}">
        </body>
    </html>
    """.format(image_cid=image_cid[1:-1]), subtype='html')

    with open(SS_DIR + '//missing_dates.png', 'rb') as img:
        maintype, subtype = mimetypes.guess_type(img.name)[0].split('/')
        msg.get_payload()[0].add_related(img.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image_cid)

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        smtp.send_message(msg)


def daterange(date1, date2):
    for n in range(int((date2 - date1).days) + 1):
        yield date1 + timedelta(n)


start_dt = datetime.date(2021, 1, 1)
end_dt = datetime.date.today()

# removing weekends
weekdays = [5, 6]
for dt in daterange(start_dt, end_dt):
    if dt.weekday() not in weekdays:
        All_date.append(dt)

# removing holidays
hol_df = pd.read_csv(METADATA_DIR + '\\holiday.csv')
hol_df['Holiday'] = pd.to_datetime(hol_df['Holiday'], format='%d-%b-%y')
All_date = pd.DataFrame(All_date)
All_date[0] = pd.to_datetime(All_date[0], format='%Y-%m-%d')
m = All_date[0].isin(hol_df['Holiday'])
All_date = All_date[~m]

# check surveillance_Invest_hist
surv_df_all = pd.read_csv(PROCESSED_DIR + '\\surveillance_Invest_hist.csv')

# ASM
filt1 = surv_df_all[surv_df_all['sub'].astype(str) == 'ASM']
Unique_Dates_filt1 = filt1['date_today'].unique()
Unique_Dates_filt1 = pd.to_datetime(Unique_Dates_filt1, format='%d-%m-%Y')
m = All_date[0].isin(Unique_Dates_filt1)
Missing_Date = All_date[~m]
if not Missing_Date.empty:
    file = 'surveillance_ASM'
    dfi.export(Missing_Date, SS_DIR + "\\missing_dates.png")
    sendmail(file)

# ST- ASM
filt2 = surv_df_all[surv_df_all['sub'].astype(str) == 'ST-ASM']
Unique_Dates_filt2 = filt2['date_today'].unique()
Unique_Dates_filt2 = pd.to_datetime(Unique_Dates_filt2, format='%d-%m-%Y')
m = All_date[0].isin(Unique_Dates_filt2)
Missing_Date = All_date[~m]
if not Missing_Date.empty:
    file = 'surveillance_ST-ASM'
    dfi.export(Missing_Date, SS_DIR + "\\missing_dates.png")
    sendmail(file)

# check bhav_data_bse_historical
bhav_data_df = pd.read_csv(PROCESSED_DIR + '\\bhav_data_bse_historical.csv')
bhav_data_df['TRADING_DATE'] = pd.to_datetime(bhav_data_df['TRADING_DATE'], format='%d-%m-%Y')
Unique_Dates = bhav_data_df['TRADING_DATE'].unique()
Unique_Dates = pd.to_datetime(Unique_Dates, format='%d-%m-%Y')
m = All_date[0].isin(Unique_Dates)
Missing_Date = All_date[~m]

if not Missing_Date.empty:
    file = 'bhav_data_bse'
    dfi.export(Missing_Date, SS_DIR + "\\missing_dates.png")
    sendmail(file)

# check bhav_data_nse_historical
bhav_data_nse = pd.read_csv(PROCESSED_DIR + '\\bhav_data_nse_historical.csv')
bhav_data_nse['secWiseDelPosDate'] = pd.to_datetime(bhav_data_nse['secWiseDelPosDate'], format='%d-%m-%Y %H:%M')
Unique_Dates = bhav_data_nse['secWiseDelPosDate'].unique()
Unique_Dates = pd.to_datetime(Unique_Dates, format='%d-%m-%Y')
m = All_date[0].isin(Unique_Dates)
Missing_Date = All_date[~m]
print('bhav_data_nse')

if not Missing_Date.empty:
    file = 'bhav_data_nse'
    dfi.export(Missing_Date, SS_DIR + "\\missing_dates.png")
    sendmail(file)
