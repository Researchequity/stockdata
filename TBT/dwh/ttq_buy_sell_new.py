#!/usr/bin/env python
import sys,os
from __builtin__ import True, False
import time
import datetime
from datetime import date
#import format
start_time = time.time()

gOrderDictionary = {}
gVerificationOrdPrice1=0;
gVerificationOrdPrice2=0;
gVerificationQty=0;
gVerificationOrderFoundFlag=False;
gSide=''
gToken = 0;
gBuyPriceDictionary = {} 
gSellPriceDictionary = {} 
gPrice=-1
gepochStartTime=0
gepochEndTime=0
gStartTime=''
gEndTime=''
gVerificationOrdPriceList = []

if len(sys.argv) == 8:
  file_name = sys.argv[1]
  gToken = sys.argv[2]
  gVerificationOrdPrice1 = sys.argv[3]
  gVerificationOrdPrice2 = sys.argv[4]
  gVerificationQty = sys.argv[5]
  gStartTime = sys.argv[6]
  gEndTime = sys.argv[7]
  
  
a=","+gToken+","
fn=gToken+".txt"

#cmd='grep %s ../../tbt-ncash-str4/DUMP_20191211_073004.DAT'%a
cmd='grep %s '%a
cmd=cmd+'%s'%file_name
cmd=cmd+'>%s'%fn
os.system(cmd)

class OrderElement():
  def __init__(self, fvOrderExId):
    self.order_id = fvOrderExId
    self.price = 0
    self.qty = 0
    self.side = ''
    
class Price():
  def __init__(self, orderPrice):
    self.orderPrice = orderPrice
    self.totalQty = 0
#    self.side = ''
#     self.buyDic = {}
#     self.sellDic = {}


def CalculationOfNeedToTradeForAOrder():
    global gBuyPriceDictionary
    global gSellPriceDictionary
    global gOrderDictionary
    global gSide
    global gVerificationOrdPrice1
    global gVerificationOrdPrice2
    global gVerificationOrdPriceList
    global gVerificationQty
    global gVerificationOrderFoundFlag
    global gToken
    totalQty = 0
    isProvidedOrdBestOrderCount = 0
    orderCounter=1
    line=1
    print_b_s_dict={}
    buy_list = []
    sell_list = []
    verificationOrdPriceList = []
    verificationOrdPriceList = range(int(gVerificationOrdPrice1),int(gVerificationOrdPrice2)+5,5)
    for orderID in sorted(gOrderDictionary.keys()):
        if gOrderDictionary[orderID].qty >= int(gVerificationQty):
        #if (gOrderDictionary[orderID].price == int(gVerificationOrdPrice1) or gOrderDictionary[orderID].price == int(gVerificationOrdPrice2) )and gOrderDictionary[orderID].qty >= int(gVerificationQty):
            for p in verificationOrdPriceList:
                if int(p) == gOrderDictionary[orderID].price:
                    if gOrderDictionary[orderID].side == 'B':
                        buy_list.append(str(orderID) + ", " +str(gOrderDictionary[orderID].price)+ ", "+ str(gOrderDictionary[orderID].qty) +", "+str(gOrderDictionary[orderID].side))
                    else:
                        sell_list.append(str(orderID) + ", " +str(gOrderDictionary[orderID].price)+ ", "+ str(gOrderDictionary[orderID].qty) +", "+str(gOrderDictionary[orderID].side))
    print "BUY SIDE                                |    SELL SIDE"   
    print "==============================================================================="         
    if len(buy_list) >  len(sell_list):
        i=0
        for i in range(len(sell_list)):
            print buy_list[i], "     | ",sell_list[i]
        for l in range(len(buy_list)-i-1):
            print buy_list[l+i+1]

    if len(sell_list) >  len(buy_list):
        i=0
        for i in range(len(buy_list)):
            print buy_list[i], "     | ",sell_list[i]
        for l in range(len(sell_list)-i-1):
            print "                                      | ", sell_list[l+i+1]
            
            
    for p in verificationOrdPriceList:
        try:                          
            print "\n\nTotal Qty Buy: ", p, gBuyPriceDictionary[int(p)].totalQty
        except KeyError:
            print "There is no buy order at the price: ", p 
        try:
            print "Total Qty Sell: ", p, gSellPriceDictionary[int(p)].totalQty 
        except KeyError:
            print "There is no sell order at the price: ", p 
            pass                         
#     print "\n\nTotal Qty Buy: ", gVerificationOrdPrice1, gBuyPriceDictionary[int(gVerificationOrdPrice1)].totalQty
#     print "Total Qty Sell: ", gVerificationOrdPrice1, gSellPriceDictionary[int(gVerificationOrdPrice1)].totalQty
#      
#     print "Total Qty Buy: ", gVerificationOrdPrice2, gBuyPriceDictionary[int(gVerificationOrdPrice2)].totalQty
#     print "Total Qty Sell: ", gVerificationOrdPrice2, gSellPriceDictionary[int(gVerificationOrdPrice2)].totalQty    
                
def newOrder(line):
    global gBuyPriceDictionary
    global gSellPriceDictionary
    global gOrderDictionary
    global gSide
    global gVerificationOrdPrice1
    global gVerificationOrderFoundFlag
    global gToken
    global gPrice
    
    lineSplitList = line.split(",")
    exOrderId = lineSplitList[7]
    orderPrice = int(lineSplitList[10])
    orderQty = int(lineSplitList[11])
    if gToken != lineSplitList[8]:
        return
    
    neworder_obj = OrderElement(exOrderId)
    neworder_obj.price = int(lineSplitList[10])
    neworder_obj.qty = int(lineSplitList[11])
    neworder_obj.side = lineSplitList[9]
    
    if neworder_obj.side == 'B':
        if orderPrice not in gBuyPriceDictionary:
            lPriceObj = Price(orderPrice)
            lPriceObj.totalQty = 0;
            gBuyPriceDictionary[orderPrice] = lPriceObj   #gBuyPriceDictionary{9014: obj}
        
        gOrderDictionary[exOrderId] = neworder_obj
        gBuyPriceDictionary[orderPrice].totalQty = gBuyPriceDictionary[orderPrice].totalQty + neworder_obj.qty;
    else:
        if orderPrice not in gSellPriceDictionary:
            lPriceObj = Price(orderPrice)
            lPriceObj.totalQty = 0;
            gSellPriceDictionary[orderPrice] = lPriceObj   #gSellPriceDictionary{9014: obj}
        
        gOrderDictionary[exOrderId] = neworder_obj
        gSellPriceDictionary[orderPrice].totalQty = gSellPriceDictionary[orderPrice].totalQty + neworder_obj.qty;
    
def modifyOrder(line):
    global gBuyPriceDictionary
    global gSellPriceDictionary
    global gOrderDictionary
    global gSide
    global gVerificationOrdPrice1
    global gVerificationOrderFoundFlag
    global gToken
    global gPrice
        
    lineSplitList = line.rstrip().split(",")
    if gToken != lineSplitList[8]:
        return
    exOrderId = lineSplitList[7]
    modifiedPrice = int(lineSplitList[10])
    modifiedQty = int(lineSplitList[11])
    side = lineSplitList[9]
    
    modifiedObj = OrderElement(exOrderId)
    modifiedObj.price = int(lineSplitList[10])
    modifiedObj.qty = int(lineSplitList[11])
    modifiedObj.side = lineSplitList[9]
    
    try:
      lPrevPriceObj = gOrderDictionary[exOrderId];
    except:
      return
            
    if side == 'B':
        gBuyPriceDictionary[lPrevPriceObj.price].totalQty = gBuyPriceDictionary[lPrevPriceObj.price].totalQty - int(lPrevPriceObj.qty)
        
        if modifiedPrice not in gBuyPriceDictionary:
            lPriceObj = Price(modifiedPrice)
            lPriceObj.totalQty = 0;
            gBuyPriceDictionary[modifiedPrice] = lPriceObj   #gBuyPriceDictionary{9014: obj}
    
        gBuyPriceDictionary[modifiedPrice].totalQty = gBuyPriceDictionary[modifiedPrice].totalQty + modifiedQty;
    else:
        gSellPriceDictionary[lPrevPriceObj.price].totalQty = gSellPriceDictionary[lPrevPriceObj.price].totalQty - int(lPrevPriceObj.qty)

        if modifiedPrice not in gSellPriceDictionary:
            lPriceObj = Price(modifiedPrice)
            lPriceObj.totalQty = 0;
            gSellPriceDictionary[modifiedPrice] = lPriceObj   #gSellPriceDictionary{9014: obj}
    
        gSellPriceDictionary[modifiedPrice].totalQty = gSellPriceDictionary[modifiedPrice].totalQty + modifiedQty;

    gOrderDictionary[exOrderId].price = modifiedPrice
    gOrderDictionary[exOrderId].qty = modifiedQty
    

#1267324,1575258571675811,1,1,1267324,T,1259745571666071199,1000000000346727,1000000000405031,9014,44700,5
def tradedOrder(line):
    global gBuyPriceDictionary
    global gSellPriceDictionary
    global gOrderDictionary
    global gSide
    global gVerificationOrdPrice1
    global gVerificationOrderFoundFlag
    global gToken
    global gPrice
    
    lineSplitList = line.rstrip().split(",")
    if gToken != lineSplitList[9]:
        return

    
    lBuyExOrderId = lineSplitList[7]
    lSellExOrderId = lineSplitList[8]
    tradedPrice = int(lineSplitList[10])
    tradedQty = int(lineSplitList[11])

    #Handle buy order
#    if lBuyExOrderId == "0" or lSellExOrderId == "0":
#        return
       
    if lBuyExOrderId in gOrderDictionary:
        lBuyObj = gOrderDictionary[lBuyExOrderId]
        gBuyPriceDictionary[lBuyObj.price].totalQty = gBuyPriceDictionary[lBuyObj.price].totalQty - tradedQty
        if lBuyObj.price == gPrice:
            print "Trade:Buy", lineSplitList[0], gBuyPriceDictionary[lBuyObj.price].totalQty,tradedQty, lBuyExOrderId
        lBuyObj.qty = lBuyObj.qty - tradedQty
        gOrderDictionary[lBuyExOrderId] = lBuyObj
         
    if lSellExOrderId in gOrderDictionary:
        lSellObj = gOrderDictionary[lSellExOrderId]
        gSellPriceDictionary[lSellObj.price].totalQty = gSellPriceDictionary[lSellObj.price].totalQty - tradedQty
        if lSellObj.price == gPrice:
            print "Trade:Sell", lineSplitList[0], gSellPriceDictionary[lSellObj.price].totalQty,tradedQty, lBuyExOrderId
        lSellObj.qty = lSellObj.qty - tradedQty
        gOrderDictionary[lSellExOrderId] = lSellObj

#1235164,1575258561501225,1,1,1235164,X,1259745561473796814,1000000000086416,9014,S,45560,7
def canceledOrder(line):
    global gBuyPriceDictionary
    global gSellPriceDictionary
    global gOrderDictionary
    global gSide
    global gVerificationOrdPrice1
    global gVerificationOrderFoundFlag
    global gToken
    global gPrice    
    
    lineSplitList = line.rstrip().split(",")
    
    if gToken != lineSplitList[8]:
        return

    exOrderId = lineSplitList[7]
    lCanPrice = int(lineSplitList[10])
    lCanQty = int(lineSplitList[11])
    lSide = lineSplitList[9]
    if exOrderId not in gOrderDictionary:
        return;

    if lSide == 'B':
        if exOrderId in gOrderDictionary:
            lBuyObj = gOrderDictionary[exOrderId]
            gBuyPriceDictionary[lBuyObj.price].totalQty = gBuyPriceDictionary[lBuyObj.price].totalQty - lCanQty
            if lBuyObj.price == gPrice:
                print "Cancel:Buy", lineSplitList[0], gBuyPriceDictionary[lBuyObj.price].totalQty,lCanQty, exOrderId
    if lSide == 'S':
        if exOrderId in gOrderDictionary:
            lSellObj = gOrderDictionary[exOrderId]
            gSellPriceDictionary[lSellObj.price].totalQty = gSellPriceDictionary[lSellObj.price].totalQty - lCanQty      
            if lSellObj.price == gPrice:
                print "Cancel:Sell", lineSplitList[0], gSellPriceDictionary[lSellObj.price].totalQty,lCanQty, exOrderId      
    del gOrderDictionary[exOrderId]
        
                
def totalQtyBeforeMe(file):
  global gepochStartTime
  global gepochEndTime
  with open(file, "r") as tokenfile:
    for line in tokenfile:
      order_type = line.split(",")[5]
      time1 = int(line.split(",")[1])
      l =  datetime.datetime.fromtimestamp(int(time1)/1000000).strftime('%c')
            
      if time1 < gepochStartTime:
          continue
      if time1 > gepochEndTime:
          print l, gepochEndTime, "Time Up"
          CalculationOfNeedToTradeForAOrder();
          sys.exit()
      if order_type == 'N':
        newOrder(line)
        
      if order_type == 'M':
        modifyOrder(line)

#       if order_type == 'T':
#         tradedOrder(line)

      if order_type == 'X':
        canceledOrder(line)

def main():
    global gVerificationOrderFoundFlag
    global gepochStartTime
    global gStartTime    
    global gepochEndTime
    global gEndTime
    print fn
    today = date.today()
    d1 = today.strftime("%d:%m:%Y")
    
    date_time = str(d1 +" "+ gStartTime)
    pattern = '%d:%m:%Y %H:%M:%S'
    gepochStartTime = int(time.mktime(time.strptime(date_time, pattern)))*1000000
    
    date_time = str(d1 +" "+ gEndTime)
    gepochEndTime = int(time.mktime(time.strptime(date_time, pattern)))*1000000
    
    totalQtyBeforeMe(fn)
    
    if gVerificationOrderFoundFlag == False:
        print "Order not found, plz provide correct exchange-order-id and Token combination."
        sys.exit(0)
        
    CalculationOfNeedToTradeForAOrder();
    #print("\nExecution Time %s Seconds ---" % (time.time() - start_time))
    
    
if __name__== "__main__":
  main()

