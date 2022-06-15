import pandas as pd
import subprocess
import dataframe_image as dfi

bhav = pd.read_csv(r'\\192.168.41.190\program\stockdata\processed\bhav_data_nse_historical.csv')
new_df1 = pd.read_csv(r'\\192.168.41.190\program\stockdata\processed\nse_stock_margin_historical_2021.csv')
new_df = pd.read_csv(r'\\192.168.41.190\program\stockdata\processed\nse_stock_margin_historical.csv')
marg = pd.concat([new_df1, new_df])
marg = marg[marg['series'].isin(["EQ","BE","SM"])]

bhav1 = pd.DataFrame(bhav)
marg1 = pd.DataFrame(marg)
bhav["bhav.X"]= pd.to_datetime(bhav['secWiseDelPosDate'], format='%d-%m-%Y %H:%M')
marg["eoldata.Date"]= pd.to_datetime(marg["date"], format='%d-%m-%Y')

result= marg1.merge(bhav1, left_on=['eoldata.Date','symbol'], right_on=['bhav.X','Stock'])
new = pd.DataFrame(result[result['symbol']=='   '])
result.to_csv(r'\\192.168.41.190\program\stockdata\raw\eqd1.csv')
print("success")

subprocess.call(r"D:/Program/R-4.1.2/bin/Rscript --vanilla D:/Program/R-4.1.2/r_ankit/stocks_cameout_ASM.R", shell=True)
subprocess.call(r"D:/Program/R-4.1.2/bin/Rscript --vanilla D:/Program/R-4.1.2/r_ankit/stocks_in_ASM_1.R", shell=True)

stocks_in_ASM_1 = pd.read_csv(r'\\192.168.41.190\program\stockdata\processed\stocks_in_ASM_1.csv')
# stocks_in_ASM_1.sort_values(by=['I'],ascending=False, inplace=True) # desc
stocks_in_ASM_1.to_csv(r'\\192.168.41.190\nilesh\stocks_in_ASM_1.csv')
stocks_in_ASM_1 = stocks_in_ASM_1.head(50)
stocks_in_ASM_1 = stocks_in_ASM_1.style.hide_index()
dfi.export(stocks_in_ASM_1, r"\\192.168.41.190\program\stockdata\screenshot\stocks_in_ASM_1.png")
stocks_cameout_ASM = pd.read_csv(r'\\192.168.41.190\program\stockdata\processed\stocks_cameout_ASM.csv')
stocks_cameout_ASM.to_csv(r'\\192.168.41.190\nilesh\stocks_cameout_ASM.csv')
# stocks_cameout_ASM.sort_values(by=['mdf.date'],ascending=False, inplace=True) # desc
stocks_cameout_ASM = stocks_cameout_ASM.head(50)
stocks_cameout_ASM = stocks_cameout_ASM.style.hide_index()
dfi.export(stocks_cameout_ASM, r"\\192.168.41.190\program\stockdata\screenshot\stocks_cameout_ASM.png")


