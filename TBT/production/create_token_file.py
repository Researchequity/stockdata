import pandas as pd
import datetime


filepath = "/home/dwh/contract_files/"
sec_filepath = filepath + "security.txt"
contract_filepath = filepath + "contract.txt"

sec_df = pd.read_csv(sec_filepath, sep='|',header=None,skiprows = 1)
sec_df = sec_df[(sec_df[2] =='EQ')|(sec_df[2] =='SM')|(sec_df[2] =='BE')]
sec_df = sec_df[(sec_df[8] ==1)]
sec_df = sec_df[[0,1,2,21]]
sec_df.columns = ['token', 'Symbol','Series','companyName']
sec_df = sec_df[~sec_df['Symbol'].str.contains('ETF')]
sec_df[['token', 'Symbol','Series']].to_csv("/home/workspace/production/python_ankit/token_security.csv",index=False)



con_df = pd.read_csv(contract_filepath, sep='|',header=None,skiprows = 1)
# con_df = con_df[(con_df[2] =='FUTSTK')|(con_df[2] =='FUTIDX')]
con_df = con_df[[0,2,3,30,53,8,7]]
con_df.columns = ['token', 'Series','Symbol','lotsize','companyName','Option','strikePrice']
con_df['strikePrice'] = con_df['strikePrice'] / 100
#con_df = con_df[con_df['companyName'].str[-6:] =='APRFUT']
con_df[['token', 'Series','Symbol','lotsize','companyName','Option','strikePrice']].to_csv("/home/workspace/production/python_ankit/token_contract.csv",index=False)


#sm_filepath = "F:\\Dumper\\Python_Ankit\\StockMetadata.csv"
#sm_df = pd.read_csv(sm_filepath)
#sm_df.drop(['token', 'companyName'], axis=1, inplace=True)
#sm_df['Series'] = sm_df['Series'].str.strip()

#final_df = sec_df.merge(sm_df, how='left', on=['Symbol','Series'])
#final_df = final_df[['Symbol','MarketCap','companyName','pdSectorInd','totalMarketCap','Series','Date_Updated','token','Sector','Industry']]
#final_df.to_csv("F:\\Dumper\\Python_Ankit\\StockMetadata.csv",index=False)
