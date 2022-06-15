import mimetypes
import smtplib
import xlwings as xw
from email.message import EmailMessage
from email.utils import make_msgid
import datetime
from utils import *
file = os.path.basename(__file__)

TODAY_DATE = datetime.date.today() #- datetime.timedelta(days=1)

python_ankit_dir = r'D:\Program\python_ankit'
Equity_Live_Data = python_ankit_dir + "\\Equity_Live_Data_NSE_BSE.xlsm"


def screenshot():
    #
    # if os.path.exists(python_ankit_dir + "\\Equity_Live_Data.xlsm"):
    #     xl = win32com.client.Dispatch("Excel.Application")
    #     xl.Workbooks.Open(python_ankit_dir + "\\Equity_Live_Data.xlsm", ReadOnly=1)
    #     xl.Application.Run("Equity_Live_Data.xlsm!module1.slicer_snap")
    #     del xl
    wb = xw.Book(python_ankit_dir + "\\Equity_Live_Data_NSE_BSE.xlsm")
    app = wb.app
    # into brackets, the path of the macro
    macro_vba = app.macro("Equity_Live_Data_NSE_BSE.xlsm!module1.slicer_snap")
    macro_vba()
    app = xw.apps.active
    app.quit()


def sendmail():
    EMAIL_ADDRESS = 'researchequity10@gmail.com'
    EMAIL_PASSWORD = 'noxtajxjdmzdjlkb'

    contacts = ['researchequity10@gmail.com', 'engineers1030164@gmail.com']
    msg = EmailMessage()
    msg['Subject'] = ' Equity Live Data as of ' + str(TODAY_DATE)
    msg['From'] = EMAIL_ADDRESS
    msg['To'] = ['rohnkoria@gmail.com','engineers1030164@gmail.com','vijitramavat@gmail.com']
    # msg['To'] = ['rohnkoria@gmail.com']

    # now create a Content-ID for the image
    image_cid = make_msgid()
    image_cid_2 = make_msgid()
    # image_cid_3 = make_msgid()
    image_cid_4 = make_msgid()
    image_cid_5 = make_msgid()
    image_cid_6 = make_msgid()
    image_cid_7 = make_msgid()
    image_cid_8 = make_msgid()
    image_cid_9 = make_msgid()
    image_cid_10 = make_msgid()

    msg.add_alternative("""\
    <!DOCTYPE html>
    <html>
        <body>
            <h1 style="color:SlateGray;">Del_gtr_1%</h1>
            <img src="cid:{image_cid}">
            <h1 style="color:SlateGray;">Del_gtr_2%</h1>
            <img src="cid:{image_cid_2}">
            <h1 style="color:SlateGray;">ATH_after_1yr_&_Pchng_5%</h1>
            <img src="cid:{image_cid_4}">
            <h1 style="color:SlateGray;">ATH_after_3yr_&_Pchng_5%</h1>
            <img src="cid:{image_cid_5}">
            <h1 style="color:SlateGray;">ATH_after_90days_&_Pchng_5%</h1>
            <img src="cid:{image_cid_6}">
            <h1 style="color:SlateGray;">ATH_after_181days_&_Pchng_5%</h1>
            <img src="cid:{image_cid_7}">
            <h1 style="color:SlateGray;">52W_after_1yr_&_Pchng_5%</h1>
            <img src="cid:{image_cid_8}">
            <h1 style="color:SlateGray;">52W_after_90days_&_Pchng_5%</h1>
            <img src="cid:{image_cid_9}">
            <h1 style="color:SlateGray;">52W_after_181days_&_Pchng_5%</h1>
            <img src="cid:{image_cid_10}">

        </body>
    </html>
    """.format(image_cid=image_cid[1:-1], image_cid_2=image_cid_2[1:-1]
               , image_cid_4=image_cid_4[1:-1], image_cid_5=image_cid_5[1:-1], image_cid_6=image_cid_6[1:-1]
               , image_cid_7=image_cid_7[1:-1], image_cid_8=image_cid_8[1:-1]
               , image_cid_9=image_cid_9[1:-1], image_cid_10=image_cid_10[1:-1]), subtype='html')

    with open(r'\\192.168.41.190\program\stockdata\screenshot\Del_gtr_1%.jpg', 'rb') as img:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image_cid)
    with open(r'\\192.168.41.190\program\stockdata\screenshot\Del_gtr_2%.jpg', 'rb') as img2:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img2.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img2.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image_cid_2)
    with open(r'\\192.168.41.190\program\stockdata\screenshot\ATH_after_1yr_&_Pchng_5%.jpg', 'rb') as img4:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img4.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img4.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image_cid_4)
    with open(r'\\192.168.41.190\program\stockdata\screenshot\ATH_after_3yr_&_Pchng_5%.jpg', 'rb') as img5:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img5.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img5.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image_cid_5)
    with open(r'\\192.168.41.190\program\stockdata\screenshot\ATH_after_90days_&_Pchng_5%.jpg', 'rb') as img6:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img6.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img6.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image_cid_6)
    with open(r'\\192.168.41.190\program\stockdata\screenshot\ATH_after_181days_&_Pchng_5%.jpg', 'rb') as img7:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img7.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img7.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image_cid_7)
    with open(r'\\192.168.41.190\program\stockdata\screenshot\Flg_52W_ATH_after_1yr_&_Pchng_5%.jpg', 'rb') as img8:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img8.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img8.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image_cid_8)
    with open(r'\\192.168.41.190\program\stockdata\screenshot\Flg_52W_ATH_after_90days_&_Pchng_5%.jpg', 'rb') as img9:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img9.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img9.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image_cid_9)
    with open(r'\\192.168.41.190\program\stockdata\screenshot\Flg_52W_ATH_after_181days_&_Pchng_5%.jpg', 'rb') as img10:
        # know the Content-Type of the image
        maintype, subtype = mimetypes.guess_type(img10.name)[0].split('/')
        # attach it
        msg.get_payload()[0].add_related(img10.read(),
                                         maintype='image',
                                         subtype='jpeg',
                                         cid=image_cid_10)

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        smtp.send_message(msg)


if __name__ == '__main__':
    try:
        screenshot()
        print('screenshot')
        sendmail()
        print('Mail Sent')
        execution_status('pass', file, '', TODAY_DATE, 1)
    except Exception as e:
        print(e)
        execution_status(str(e), file, '', TODAY_DATE, 0)




