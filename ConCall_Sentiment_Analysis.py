from sklearn.feature_extraction.text import CountVectorizer
import pdfplumber as pdf
import os
import regex
import re
import pandas as pd
import nltk
import easygui
import win32wnet
import numpy as np
import datetime
from datetime import datetime
import time
import PyPDF2
import glob
from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA
from tkinter import filedialog
import tkinter as tk
directory = filedialog.askdirectory()
s1= directory[0:2]
if s1 == "D:" :
    print("Directory is - " + str(s1))
else:
    repl = win32wnet.WNetGetConnection(s1)
    repl = repl.replace('\\', '/')
    path = directory.replace(s1, repl)
    directory = directory.replace(s1, repl)
path = directory + "/**/*.txt"
files= glob.glob(path)

fz= pd.DataFrame(files)
z= len(files)
k=0
file = files
filesprocessed = []
x=files[24]
for x in file:
    k=k+1
    df = open(x, 'r+', encoding='latin')
    df = df.read()
    text = str(df)
    rep = pd.read_csv(r"//192.168.41.190/nilesh/concall/Replace.csv")
    close = True
    rep = rep.dropna(subset=['Keyword'])
    for i in range(len(rep)):
        for i in range(len(rep)):
            repl = str(rep['Replace'].iloc[i])
            subs = str(rep['Keyword'].iloc[i])
            compiled = re.compile(re.escape(subs), re.IGNORECASE)
            text = compiled.sub(repl, text)
        rep = pd.read_csv((r"//192.168.41.190/nilesh/concall/Replace.csv"))
        rep = rep.dropna(subset=['Keyword'])
        KEYWORD = rep['Keyword'].iloc[i]
        #KEYWORD = easygui.enterbox("Enter the Keyword to search pattern")
        m = re.compile(f'{KEYWORD}[,|\s]*(\w+)')
        words = m.findall(text)
        words = [KEYWORD + " " + sub for sub in words]
        words = list(set(words))
        if len(words) > 1:
            xword = easygui.multchoicebox("Select Words to add in delete list", choices=words)
            xword = pd.DataFrame(xword)
        else:
            xword = []
            if len(words) > 0:
                words.append(None)
                words.append(None)
                xword.append(easygui.choicebox("Select Words to add in delete list", choices=words))
                xword = pd.DataFrame(xword)
        if len(xword) > 0:
            xword.columns = ['Keyword']
            xword['S.no'] = xword.index
            xword['Pos'] = 0
            xword['Neg'] = 0
            xword['Compound'] = 0
            xword['Replace'] = ''
            xword = xword.reindex(['S.no', 'Keyword', 'Pos', 'Neg', 'Compound', 'Replace'], axis=1)
            rep = pd.concat([rep, xword])
            rep = rep.drop_duplicates(subset=['Keyword'])
            rep.to_csv(r"//192.168.41.190/nilesh/concall/Replace.csv", index=False)

    data = text.split(". ")
    data = pd.DataFrame(data)
    rep = rep.dropna(subset=['Keyword'])
    data.columns = ['text']
    def check(data):
        data1 = data
        results = []
        data1 = pd.DataFrame(data1)
        data1.columns = ['Col1']
        for headline in data1['Col1']:
            pol_score = SIA().polarity_scores(headline)  # run analysis
            pol_score['headline'] = headline  # add headlines for viewing
            results.append(pol_score)
        results
        res = pd.DataFrame(results)
        return (res)
    xx = check(data)
    file = os.path.basename(x)
    filename = str(file).replace(".txt", "")
    fname = filename.split("_")
    fname = fname[0]
    qtr = filename[-8:]
    if fname.isnumeric():
      scode = fname
      cck = 0
    elif fname.isalpha():
      Com = fname
      cck=1
    xx.neg.sum()
    if cck==0:
      Com = scode
    file = str("//192.168.41.190/nilesh/concall/")+str(filename)
    file = str(file) + ".csv"
    xx.to_csv(file)
    qtr = qtr.replace("_","-")
    try:
        Date = pd.to_datetime(qtr, format="%b-%Y")
    except:
        print(i)
    x = pd.DataFrame([[Com,filename,  xx.neg.sum(), xx.neu.sum(),xx.pos.sum(), xx.compound.sum(), qtr,Date]])
    x.columns=['Company','File Name', 'Negative', 'Neutral', 'Positive', 'Compound','Qtr','Date']
    status = int(z-k)
    print(str(status) + " Files Left")
    path = r"//192.168.41.190/nilesh/NSA_Score.csv"
    score = pd.read_csv(path)
    score = pd.concat([score, x])
    score = score.drop_duplicates(subset='File Name', keep = 'last')
    score.to_csv(path, index=False)
while close == True:
    #KEYWORD = rep['Keyword'].ilo
    #f
    # [i]
    for i in range(len(rep)):
        repl = str(rep['Replace'].iloc[i])
        subs = str(rep['Keyword'].iloc[i])
        compiled = re.compile(re.escape(subs), re.IGNORECASE)
        text = compiled.sub(repl, text)
    KEYWORD = easygui.enterbox("Enter the Keyword to search pattern")
    m = re.compile(f'{KEYWORD}[,|\s]*(\w+)')
    words = m.findall(text)
    words = [KEYWORD + " " + sub for sub in words]
    words = list(set(words))
    if len(words) > 1:
        xword = easygui.multchoicebox("Select Words to add in delete list", choices=words)
        xword = pd.DataFrame(xword)
    else:
        xword = []
        if len(words) > 0 :
            words.append(None)
            words.append(None)
            xword.append(easygui.choicebox("Select Words to add in delete list", choices=words))
            xword = pd.DataFrame(xword)
    if len(xword) > 0:
        xword.columns = ['Keyword']
        xword['S.no'] = xword.index
        xword['Pos'] = 0
        xword['Neg'] = 0
        xword['Compound'] = 0
        xword['Replace'] = ''
        xword.columns
        xword = xword.reindex(['S.no', 'Keyword', 'Pos', 'Neg', 'Compound', 'Replace'], axis=1)
        rep = pd.concat([rep, xword])
        rep = rep.drop_duplicates(subset=['Keyword'])
        rep.to_csv(r"//192.168.41.190/nilesh/concall/Replace.csv", index=False)
    close = easygui.ccbox("Do You want to countinue Adding More words ?")

    if close == False:
        break

import pandas as pd
import datetime as dt
import numpy as np
import re

data = pd.read_csv(r'//192.168.41.190/nilesh/NSA_Score.csv')
for i in range(len(mon)):
  data.Qtr = data.Qtr.str.replace(mon.Mon[i],mon.Num[i],regex=True )
print('Done')
smd = pd.read_csv('D:/R/Data/StockMetaData.csv')
data1= data.iloc[:,4:7]
data1= data1.iloc[2:]
data1.Qtr = data1.Qtr.str.lower()
data.Date =  data.Date.str[:11]
data.to_csv("D:/R/Data/try.csv")
data.Date = pd.to_datetime(data.Date)

data = data.sort_values(by=['Company','Date'], ascending=True)
comp_dat = data.drop_duplicates(subset=['Company'])
num = ['01','02','03','04','05','06','07','08','09','10','11','12']
alp = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
mon = pd.DataFrame({'Mon':alp,'Num':num})


data1.Qtr = data1.Qtr.str.replace('-','', regex=True)
data1 = data1[data1.Qtr.astype(str).str.isdigit()]
data1['Mon']=data1.Qtr.str[:2]
data1['Year']=data1.Qtr.str[-4:]
data1['Qtr']= data1.Mon + "-" + data1.Year
data1['Qtr']= pd.to_datetime(data1['Qtr'], format = "%m-%Y")
data1['Mon']=data1['Qtr'].dt.strftime('%m')
data1['Year']=data1['Qtr'].dt.strftime('%Y')
data1['Mon'] = data1['Mon'].astype('int64')
data1['qtr'] = np.where(data1['Mon']>9,'11',np.where(data1['Mon']>6,'08',np.where(data1['Mon']>3,'04','02')))

data1['Qtr1']= data1['Year']+ "-" + data1['qtr']
data1.drop_duplicates(subset = ['Company','Qtr1'],inplace=True)


data1.dtypes
data1= data1.iloc[:,[0,2,6]]
data2 = data1.pivot(index='Company',columns='Qtr1', values='Compound')
data3 = data2[data2.columns[::-1]]
data3 = data3.fillna(" ")
data3['script_code']=data3.index


smd.script_code = smd.script_code.astype('str')
smd.script_code = smd.script_code.str[:-2]
data4 = pd.merge(smd[[ 'companyName','script_code']],data3, on = 'script_code', how='right' )
data4 = pd.merge(data4,comp_dat[['Company','Date']], left_on='script_code', right_on='Company',how='left')
date = data4.pop('Date')
data4.insert(2,'Date',date)
data4.to_csv(r"//192.168.41.190/nilesh/NFinal SA Score.csv")

zzz = pd.DataFrame(filesprocessed)
print(filesprocessed)
zzz = pd.DataFrame(filesprocessed)
zzz.to_csv("D:/Filesprocessed.csv")
easygui.msgbox("Thanks for Using Sentiment Analysis")
