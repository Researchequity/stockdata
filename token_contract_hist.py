import re

import pandas as pd

from utils import *
date_today = datetime.date.today() #- datetime.timedelta(days=10)
file = os.path.basename(__file__)
str_date = date_today.strftime('%d%m%y')
print(str_date)

jii
def token_hist():
    token = pd.read_csv(METADATA_DIR + '\\token_contract.csv')
    token = token[token['companyName'].notna()]
    token["companyName"] = token.apply(lambda row: re.sub('^' + row.Symbol + r'\s*', '', row.companyName),axis=1)
    token['test'] = token['strikePrice'].astype(int).astype(str) + token['Option']
    token["companyName"] = token.apply(lambda row: re.sub(r'\s*' + row.test, '', row.companyName), axis=1)

    token_opt = token[token.Option != 'XX']
    token_opt['test'] = token_opt['strikePrice'].astype(float).astype(str) + token_opt['Option']
    token_opt["companyName"] = token_opt.apply(lambda row: re.sub(r'\s*' + row.test, '', row.companyName), axis=1)
    token_opt.drop(['test'], inplace=True, axis=1)

    token_fut = token[token.Option == 'XX']
    token_fut['test'] = token_fut['Option'].str.replace('XX', 'FUT')
    token_fut["companyName"] = token_fut.apply(lambda row: re.sub(r'\s*' + row.test, '', row.companyName), axis=1)
    token_fut.drop(['test'], inplace=True, axis=1)

    token = pd.concat([token_opt, token_fut])

    li_final = []
    for index, s in token.iterrows():
        print(s.tolist())

        if s['companyName'][4].isnumeric() == True:
            s['exp_date'] = pd.to_datetime(s['companyName'], format='%y%m%d')
            s['exp_month'] = 'ne'
            li_final.append(s.tolist())

        else:
            s['exp_date'] = pd.to_datetime(s['companyName'], format='%y%b')
            s['exp_month'] = 'me'
            li_final.append(s.tolist())

    df = pd.DataFrame(li_final, columns=['token', 'Series', 'Symbol', 'lotsize', 'companyName', 'Option', 'strikePrice'
                                         , 'exp_date', 'exp_month'])

    df.to_csv(METADATA_DIR + '\\token_contract\\token_contract_hist' + str_date + '.csv', index=False)
    df['date_today'] = date_today

    if os.path.exists(METADATA_DIR + '\\token_contract_hist.csv'):
        token_hist = pd.read_csv(METADATA_DIR + '\\token_contract_hist.csv')
        token_hist['exp_date'] = pd.to_datetime(token_hist['exp_date'], format='%d-%m-%Y')
        historical = pd.concat([df, token_hist])
        historical['exp_date'] = historical['exp_date'].dt.strftime('%d-%m-%Y')
        historical.to_csv(METADATA_DIR + '\\token_contract_hist.csv', index=False)
    else:
        df['exp_date'] = df['exp_date'].dt.strftime('%d-%m-%Y')
        df.to_csv(METADATA_DIR + '\\token_contract_hist.csv', index=False)

    # check and drop duplicates
    dups = pd.read_csv(METADATA_DIR + '\\token_contract_hist.csv')
    dups = dups.drop_duplicates(
        subset=['token', 'Series', 'Symbol', 'lotsize', 'Option', 'strikePrice'])
    dups = dups[dups['companyName'].notna()]
    dups.to_csv(METADATA_DIR + '\\token_contract_hist.csv', index=False)


if __name__ == '__main__':
    token_hist()
