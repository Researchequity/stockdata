import dataframe_image as dfi
import mimetypes
from email.utils import make_msgid
from utils import *
from email.mime.text import MIMEText
import time

TODAY_DATE = datetime.date.today()
file = os.path.basename(__file__)
linkarray = []


def sendmail_fund(pdf_link,df,sub):

    EMAIL_ADDRESS = 'researchequity10@gmail.com'
    EMAIL_PASSWORD = 'noxtajxjdmzdjlkb'
    contacts = ['researchequity10@gmail.com', 'engineers1030164@gmail.com']

    msg = EmailMessage()
    msg['Subject'] = sub + 'as of:' + str(TODAY_DATE)
    msg['From'] = EMAIL_ADDRESS
    msg['To'] = ['rohnkoria@gmail.com','engineers1030164@gmail.com','vijitramavat@gmail.com',
                 'researchequity10@gmail.com', 'chayanbaxi1990@gmail.com']
    # msg['To'] = ['rohnkoria@gmail.com']

    for l in range(0, len(pdf_link)):
        names = df.iloc[l]['name']
        a = f'<a href="{pdf_link[l]}">{names}</a>'
        linkarray.append(a)
    df['name'] = linkarray
    df=df[['name','Exchange_Received']]
    body="""\
    <!DOCTYPE html>
    <html>
    <br>
    <br>
        <body>
         <br>"""+df.to_html(classes='table table-stripped',escape=False,index=False,border=None)+"""</body>
    </html>"""

    data = MIMEText(body, "html")
    msg.set_content(data)

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        smtp.send_message(msg)


def sendmail(pdf_link,df,sub):

    EMAIL_ADDRESS = 'researchequity10@gmail.com'
    EMAIL_PASSWORD = 'noxtajxjdmzdjlkb'
    contacts = ['researchequity10@gmail.com', 'engineers1030164@gmail.com']

    msg = EmailMessage()
    msg['Subject'] = sub + 'as of:' + str(TODAY_DATE)
    msg['From'] = EMAIL_ADDRESS
    msg['To'] = ['rohnkoria@gmail.com','engineers1030164@gmail.com','vijitramavat@gmail.com','researchequity10@gmail.com']
    # msg['To'] = ['rohnkoria@gmail.com']

    for l in range(0, len(pdf_link)):
        names = df.iloc[l]['name']
        a = f'<a href="{pdf_link[l]}">{names}</a>'
        linkarray.append(a)
    df['name'] = linkarray
    df=df[['name','Exchange_Received']]
    body="""\
    <!DOCTYPE html>
    <html>
    <br>
    <br>
        <body>
         <br>"""+df.to_html(classes='table table-stripped',escape=False,index=False,border=None)+"""</body>
    </html>"""

    data = MIMEText(body, "html")
    msg.set_content(data)

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        smtp.send_message(msg)


def announcement():
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=browser_profile())
    driver.get("https://www.bseindia.com/corporates/ann.html")
    driver.minimize_window()
    pd.set_option('display.max_columns', None)
    tables = len(driver.find_elements_by_xpath('//*[@id="lblann"]/table/tbody/tr[4]/td/table'))
    rows = len(driver.find_elements_by_xpath('//*[@id="lblann"]/table/tbody/tr[4]/td/table[1]/tbody/tr'))
    cols = len(driver.find_elements_by_xpath('//*[@id="lblann"]/table/tbody/tr[4]/td/table[1]/tbody/tr[1]/td'))
    ic(tables)
    table_df = []

    pagenumber = 1
    try:
        while pagenumber <= 4:
            if (pagenumber > 1):
                time.sleep(3)
                page_link = driver.find_element_by_xpath('//a[@id="idnext"][1]')
                page_link.click()
                time.sleep(5)

            print("Page number is :", pagenumber)
            pagenumber = pagenumber + 1

            # Printing the data of the table
            for n in range(1, tables + 1):
                for r in range(1, rows - 1):
                    for p in range(1, cols - 2):
                        # obtaining the text from each column of the table
                        value = driver.find_element_by_xpath(
                            '//*[@id="lblann"]/table/tbody/tr[4]/td/table['+str(n)+']/tbody/tr['+str(r)+']/td['+str(p)+']').text
                        value_2 = driver.find_element_by_xpath(
                            '//*[@id="lblann"]/table/tbody/tr[4]/td/table['+str(n)+']/tbody/tr[2]').text
                        try:
                            pdf = driver.find_element_by_xpath(
                                '//*[@id="lblann"]/table/tbody/tr[4]/td/table['+str(n)+']/tbody/tr['+str(r)+']/td[3]/a').get_attribute('href')
                        except:
                            continue

                        print(value_2)
                        print(pdf)
                        table_df.append(value)
                        table_df.append(value_2)
                        table_df.append(pdf)
                        table_df_csv = pd.DataFrame(table_df)
                        print()
    except:
        pass
    driver.close()
    return table_df_csv


def calculations(table_df_csv):
        table_df_csv = pd.DataFrame(table_df_csv.values.reshape(-1, 3),
                                    columns=['name', 'Exchange_Received', 'pdf_link'])
        table_df_csv['script_code'] = table_df_csv['name'].str.split(' - ').str[1].str.strip()
        table_df_csv['Exchange_Received'] = \
            table_df_csv['Exchange_Received'].str.split('Exchange').str[1].str.strip().str.split('Time').str[
                1].str.strip()
        table_df_csv['Exchange_Received'] = pd.to_datetime(table_df_csv['Exchange_Received'],
                                                           format='%d-%m-%Y %H:%M:%S')

        table_df_csv['latest_date_time'] = table_df_csv['Exchange_Received'].max()
        latest_date_time = pd.to_datetime(table_df_csv['latest_date_time'], format='%d-%m-%Y %H:%M:%S')
        table_df_csv['latest_date_time'] = latest_date_time

        if not os.path.exists(METADATA_DIR + '\\announcement.csv'):
            table_df_csv['Exchange_Received'] = table_df_csv['Exchange_Received'].dt.strftime('%d-%m-%Y %H:%M')
            table_df_csv['latest_date_time'] = table_df_csv['latest_date_time'].dt.strftime('%d-%m-%Y %H:%M')
            table_df_csv.to_csv(METADATA_DIR + '\\announcement.csv', index=False)
        else:
            Historical = pd.read_csv(METADATA_DIR + '\\announcement.csv')
            Historical['latest_date_time'] = pd.to_datetime(Historical['latest_date_time'],
                                                            format='%d-%m-%Y %H:%M')
            Historical['Exchange_Received'] = pd.to_datetime(Historical['Exchange_Received'],
                                                             format='%d-%m-%Y %H:%M')

            latest_date = Historical['latest_date_time'].unique().max()
            ic(latest_date)
            check = table_df_csv[table_df_csv['Exchange_Received'] > latest_date]

            if not check.empty:
                Historical = pd.concat([check, Historical])
                Historical['Exchange_Received'] = Historical['Exchange_Received'].dt.strftime('%d-%m-%Y %H:%M')
                Historical['latest_date_time'] = Historical['latest_date_time'].dt.strftime('%d-%m-%Y %H:%M')
                Historical = Historical[~Historical['name'].str.contains('Duplicate Certificate')]
                Historical = Historical[~Historical['name'].str.contains('Closure of Trading Window')]
                Historical = Historical[~Historical['name'].str.contains('Debt Securities')]
                Historical = Historical[~Historical['name'].str.contains('Tax deduction at source')]
                fund_raise_csv = Historical[(Historical['name'].str.contains('Fund Raising'))]
                fund_raise_csv.to_csv(RAW_DIR + '\\fund_raise.csv', index=False)
                Historical.to_csv(METADATA_DIR + '\\announcement.csv', index=False)
                super_scripts_df = pd.read_csv(METADATA_DIR + '\\announcement_superScripts.csv',
                                               encoding='unicode_escape')[['script_code']]

                super_scripts_list = super_scripts_df['script_code'].astype(str)
                super_scripts = check[(check['script_code'].isin(super_scripts_list))]

                super_scripts = pd.DataFrame(super_scripts)
                super_scripts = super_scripts[~super_scripts['name'].str.contains('Duplicate Certificate')]
                super_scripts = super_scripts[~super_scripts['name'].str.contains('Closure of Trading Window')]
                super_scripts = super_scripts[~super_scripts['name'].str.contains('Debt Securities')]
                super_scripts = super_scripts[~super_scripts['name'].str.contains('Tax deduction at source')]
                pdf_link = super_scripts['pdf_link']
                pdf_link = pdf_link.tolist()
                super_scripts.drop(['pdf_link', 'latest_date_time', 'script_code'], inplace=True, axis=1)
                ic(super_scripts)

                if not super_scripts.empty:
                    sub = 'Announcement'
                    sendmail(pdf_link,super_scripts,sub)
                    print('mail sent')

                super_scripts = check[(check['name'].str.contains('Fund Raising'))]
                super_scripts = super_scripts[~super_scripts['name'].str.contains('Debt Securities')]
                super_scripts = pd.DataFrame(super_scripts)
                pdf_link = super_scripts['pdf_link']
                pdf_link = pdf_link.tolist()
                super_scripts.drop(['pdf_link', 'latest_date_time', 'script_code'], inplace=True, axis=1)
                ic(super_scripts)
                if not super_scripts.empty:
                    sub = 'Fund Raising'
                    sendmail_fund(pdf_link,super_scripts,sub)
                    print('mail sent')


if __name__ == '__main__':
    try:
        table_df_csv = announcement()
        calculations(table_df_csv)
        execution_status('pass', file, logging.ERROR, TODAY_DATE, 1)
    except Exception as e:
        print(e)
        execution_status(str(e), file, logging.ERROR, TODAY_DATE, 0)
        sendmail_err(file, e)















