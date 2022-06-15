from utils import *
from icecream import ic


file = os.path.basename(__file__)


def scrap_table_data():
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=browser_profile())
    driver.get("https://www.bseindia.com/corporates/Forth_Results.html")
    pd.set_option('display.max_columns', None)
    rows = len(driver.find_elements_by_xpath(
        '/html/body/div[5]/div/div/div/table/tbody/tr/td/table/tbody/tr'))
    cols = len(driver.find_elements_by_xpath(
        '/html/body/div[5]/div/div/div/table/tbody/tr/td/table/tbody/tr[1]/td'))
    ic(rows)
    ic(cols)
    table_df = []
    # Printing the data of the table
    for r in range(2, rows + 1):  #
        for p in range(1, cols + 1):
            # obtaining the text from each column of the table
            value = driver.find_element_by_xpath(
                "/html/body/div[5]/div/div/div/table/tbody/tr/td/table/tbody/tr[" + str(
                    r) + "]/td[" + str(p) + "]").text
            print(value)
            table_df.append(value)
            results_calendar_csv = pd.DataFrame(table_df)
        print()

    results_calendar_csv = pd.DataFrame(results_calendar_csv.values.reshape(-1, 3),
                                        columns=['Security Code', 'Security Name', 'Result Date'])
    results_calendar_csv.to_csv(METADATA_DIR + '\\results_calendar.csv', index=False)
    driver.close()


if __name__ == '__main__':
    try:
        scrap_table_data()
        execution_status('pass', file, logging.ERROR, '', 1)
    except Exception as e:
        print(e)
        execution_status(str(e), file, logging.ERROR, '', 0)
        sendmail_err(file, e)
