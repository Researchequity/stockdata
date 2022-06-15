import pandas as pd

df_chunk = pd.read_csv('D:\\DUMP_20200114_080002.DAT',chunksize=7000)

for df_bse in df_chunk:
    print(df_bse.head())
    df_bse.to_csv('D:\\DUMP_20200114_080002_10000.DAT')
    print('here')
