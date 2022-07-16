#!/usr/bin/env python
import sys,os
from __builtin__ import True, False
import time
import datetime
from datetime import date
from collections import OrderedDict
#import format
start_time = time.time()

gOrderDictionary = {}
gVerificationOrdPrice1=0;
gVerificationOrdPrice2=0;
gVerificationQty=0;
gVerificationOrderFoundFlag=False;
gSide=''
gToken = 0;
gFilterprice = 0
gBuyPriceDictionary = {} 
gSellPriceDictionary = {} 
gPrice=-1
gepochStartTime=0
gepochEndTime=0
gStartTime=''
gEndTime=''
gVerificationOrdPriceList = []
gBuyPendingQty = 0
gSellPendingQty = 0
gTotalTradedQuantity = 0
gDepth = 0

gOrderID = ""

if len(sys.argv) == 3:
  file_name = sys.argv[1]
  gOrderID = sys.argv[2]
  
#1267324,1575258571675811,1,1,1267324,T,1259745571666071199,1000000000346727,1000000000405031,9014,44700,5
      
def totalQtyBeforeMe(file):
    global gOrderID
    with open(file, "r") as tokenfile:
        for line in tokenfile:
            lineSplitList = line.rstrip().split(",")
            order_type = line.split(",")[5]
            exOrderId = line.split(",")[7]
            time1 = int(line.split(",")[1])
            l =  (datetime.datetime.fromtimestamp(int(time1)/1000000).strftime('%c')).split(" ")[3]
            if exOrderId == gOrderID:
                print lineSplitList[0],",",l,",",lineSplitList[5],",",lineSplitList[7],",",lineSplitList[8],",",lineSplitList[9],",",lineSplitList[10],",",lineSplitList[11]

def main():
    global file_name
    totalQtyBeforeMe(file_name)
    
    
if __name__== "__main__":
  main()


