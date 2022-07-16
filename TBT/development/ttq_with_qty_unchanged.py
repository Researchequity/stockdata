#!/usr/bin/env python
import sys,os

from __builtin__ import True, False
import time
import datetime
from datetime import date
#import format
start_time = time.time()

gOrderDictionary = {}
gVerificationOrdPrice=0;
gVerificationOrderFoundFlag=False;
gInputExOrderId=0;
gSide=''
gToken = 0;
gBuyPriceDictionary = {} 
gSellPriceDictionary = {} 
gPrice=-1


if len(sys.argv) == 5:
  file_name = sys.argv[1]
  gToken = sys.argv[2]
  gQty = sys.argv[3]
  gInputExOrderId = sys.argv[4]
  
if len(sys.argv) == 6:
  file_name = sys.argv[1]
  gToken = sys.argv[2]
  gInputExOrderId = sys.argv[3]
  gPrice=int(sys.argv[4])
  

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
    global gVerificationOrdPrice
    global gVerificationOrderFoundFlag
    global gInputExOrderId
    global gToken
    global gQty
    totalQty = 0

    isProvidedOrdBestOrderCount = 0
    orderCounter=1
    
    #print  '{:<20}{:<10}{:<5}{:<5}{:<15}'.format("OrderID","Price","OrderType","B/S(er)","QtyRemain")
    if gSide == 'B':
        print '{:<20}{:<10}{:<10}{:<10}'.format("OrderID","Price","BUYER","QtyRemain")
        
        for priceKey in sorted(gBuyPriceDictionary.keys(), reverse=True):
            if int(gVerificationOrdPrice) <= priceKey: # and gBuyPriceDictionary[priceKey].before_your_order == 'n':
                if gBuyPriceDictionary[priceKey].totalQty > 0:
                    for orderID in sorted(gOrderDictionary.keys()):
                        if gOrderDictionary[orderID].price <  gVerificationOrdPrice or orderID == gInputExOrderId or gOrderDictionary[orderID].qty == 0:
                            continue
			if gOrderDictionary[orderID].side == 'S':
			    continue
			if int(gOrderDictionary[orderID].qty) >= int(gQty):
                            print '{:<20}{:<10}{:<10}{:<10}'.format(str(gOrderDictionary[orderID].order_id), str(gOrderDictionary[orderID].price), str(orderCounter), str(gOrderDictionary[orderID].qty))
                        orderCounter = orderCounter + 1
                        if orderCounter >= 100:
                            pass
                    isProvidedOrdBestOrderCount = isProvidedOrdBestOrderCount + 1
                    totalQty = totalQty + gBuyPriceDictionary[priceKey].totalQty
            else:
                break;
        
        print "\n\nTotal Buyers remain:", orderCounter - 1
        print "Total Qty Remain:", (totalQty - gOrderDictionary[gInputExOrderId].qty)  
    
    if gSide == 'S':
	print  '{:<20}{:<10}{:<10}{:<10}'.format("OrderID","Price","SELLER","QtyRemain")
	
        for priceKey in sorted(gSellPriceDictionary.keys()):
            if int(gVerificationOrdPrice) >= priceKey:
                if gSellPriceDictionary[priceKey].totalQty > 0:
                    for orderID in sorted(gOrderDictionary.keys() ):
                        if gOrderDictionary[orderID].price >  gVerificationOrdPrice or orderID == gInputExOrderId or gOrderDictionary[orderID].qty == 0:
                            continue
			if gOrderDictionary[orderID].side == 'B':
			    continue
			if int(gOrderDictionary[orderID].qty) >= int(gQty):
			    print '{:<20}{:<10}{:<10}{:<10}'.format(str(gOrderDictionary[orderID].order_id), str(gOrderDictionary[orderID].price), str(orderCounter), str(gOrderDictionary[orderID].qty))
                        orderCounter = orderCounter + 1
                    totalQty = totalQty + gSellPriceDictionary[priceKey].totalQty
            else:
                break;
               
        #print "SELL:At-Price:", gVerificationOrdPrice, "Remaing Qty :", totalQty, "Need to trade-qty:", (totalQty - gOrderDictionary[gInputExOrderId].qty) 
        print "\n\nTotal Sellers remain:", orderCounter - 1
        print "Total Qty Remain:", (totalQty - gOrderDictionary[gInputExOrderId].qty) 
    print("\nExecution Time %s Seconds" % (time.time() - start_time))
    #sys.exit()
    
    
def newOrder(line):
    global gBuyPriceDictionary
    global gSellPriceDictionary
    global gOrderDictionary
    global gSide
    global gVerificationOrdPrice
    global gVerificationOrderFoundFlag
    global gInputExOrderId
    global gToken
    global gPrice
    
    lineSplitList = line.split(",")
    exOrderId = lineSplitList[7]
    orderPrice = int(lineSplitList[10])
    if gToken != lineSplitList[8]:
        return
    
    if gVerificationOrderFoundFlag == True:
        if gSide == 'B' and orderPrice <= gVerificationOrdPrice:
            return;
        if gSide == 'S' and orderPrice >= gVerificationOrdPrice:
            return;
    
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
        if orderPrice == gPrice:
            print "New:Buy", lineSplitList[0], gBuyPriceDictionary[orderPrice].totalQty,neworder_obj.qty, exOrderId
    else:
        if orderPrice not in gSellPriceDictionary:
            lPriceObj = Price(orderPrice)
            lPriceObj.totalQty = 0;
            gSellPriceDictionary[orderPrice] = lPriceObj   #gSellPriceDictionary{9014: obj}
        
        gOrderDictionary[exOrderId] = neworder_obj
        gSellPriceDictionary[orderPrice].totalQty = gSellPriceDictionary[orderPrice].totalQty + neworder_obj.qty;
        if orderPrice == gPrice:
            print "New:Sell", lineSplitList[0], gSellPriceDictionary[orderPrice].totalQty,neworder_obj.qty, exOrderId
    
        
#     print gInputExOrderId, exOrderId
    if exOrderId == gInputExOrderId:
        gVerificationOrdPrice = orderPrice
        gSide = lineSplitList[9]
        gVerificationOrderFoundFlag = True; 
        gOrderDictionary[exOrderId] = neworder_obj
        print line
	exit



def modifyOrder(line):
    global gBuyPriceDictionary
    global gSellPriceDictionary
    global gOrderDictionary
    global gSide
    global gVerificationOrdPrice
    global gVerificationOrderFoundFlag
    global gInputExOrderId
    global gToken
    global gPrice
        
    lineSplitList = line.rstrip().split(",")
    if gToken != lineSplitList[8]:
        return

    #print lineSplitList
    exOrderId = lineSplitList[7]
    modifiedPrice = int(lineSplitList[10])
    modifiedQty = int(lineSplitList[11])
    side = lineSplitList[9]
    
    modifiedObj = OrderElement(exOrderId)
    modifiedObj.price = int(lineSplitList[10])
    modifiedObj.qty = int(lineSplitList[11])
    modifiedObj.side = lineSplitList[9]
    
    if exOrderId == gInputExOrderId:
        print "This program not handled Modify-event for provided ex-order-id."
        sys.exit()
        

    if exOrderId not in gOrderDictionary:
        if gVerificationOrderFoundFlag == True and ((gSide == 'B' and modifiedPrice <= gVerificationOrdPrice) or (gSide == 'S' and modifiedPrice >= gVerificationOrdPrice)):
            return;

        if modifiedObj.side == 'B':
            if modifiedPrice not in gBuyPriceDictionary:
                lPriceObj = Price(modifiedPrice)
                lPriceObj.totalQty = 0;
                gBuyPriceDictionary[modifiedPrice] = lPriceObj   #gBuyPriceDictionary{9014: obj}
    
            gOrderDictionary[exOrderId] = modifiedObj
            gBuyPriceDictionary[modifiedPrice].totalQty = gBuyPriceDictionary[modifiedPrice].totalQty + modifiedObj.qty;
            if modifiedPrice == gPrice:
                print "Mod:added in dic modP > OrderP ", lineSplitList[0], gBuyPriceDictionary[modifiedPrice].totalQty,modifiedQty
            return
        else:
            if modifiedPrice not in gSellPriceDictionary:
                lPriceObj = Price(modifiedPrice)
                lPriceObj.totalQty = 0;
                gSellPriceDictionary[modifiedPrice] = lPriceObj   #gSellPriceDictionary{9014: obj}
    
            gOrderDictionary[exOrderId] = modifiedObj
            gSellPriceDictionary[modifiedPrice].totalQty = gSellPriceDictionary[modifiedPrice].totalQty + modifiedObj.qty;
            if modifiedPrice == gPrice:
                print "Mod:added in dic modP < OrderP ", lineSplitList[0], gSellPriceDictionary[modifiedPrice].totalQty,modifiedQty
            return
        
    
    lPrevPriceObj = gOrderDictionary[exOrderId];
            
    if side == 'B':
        gBuyPriceDictionary[lPrevPriceObj.price].totalQty = gBuyPriceDictionary[lPrevPriceObj.price].totalQty - int(lPrevPriceObj.qty)
        if modifiedPrice == gPrice:
            print "Mod(previousObj)", lineSplitList[0], gBuyPriceDictionary[lPrevPriceObj.price].totalQty,lPrevPriceObj.qty,modifiedQty
        
        if gVerificationOrderFoundFlag == True and modifiedPrice < gVerificationOrdPrice :
            del gOrderDictionary[exOrderId]
            return
#deleted for Priority change       
        if gVerificationOrderFoundFlag == True :
            if modifiedPrice == gVerificationOrdPrice:
                if lPrevPriceObj.price != modifiedPrice or (lPrevPriceObj.price == modifiedPrice and lPrevPriceObj.qty < modifiedQty):
                    del gOrderDictionary[exOrderId]
                    return
       
        if modifiedPrice not in gBuyPriceDictionary:
            lPriceObj = Price(modifiedPrice)
            lPriceObj.totalQty = 0;
            gBuyPriceDictionary[modifiedPrice] = lPriceObj   #gBuyPriceDictionary{9014: obj}
    
        
        gBuyPriceDictionary[modifiedPrice].totalQty = gBuyPriceDictionary[modifiedPrice].totalQty + modifiedQty;
        if modifiedPrice == gPrice:
            print "Mod:Buy", lineSplitList[0], gBuyPriceDictionary[modifiedPrice].totalQty,lPrevPriceObj.qty,modifiedQty
    else:
        gSellPriceDictionary[lPrevPriceObj.price].totalQty = gSellPriceDictionary[lPrevPriceObj.price].totalQty - int(lPrevPriceObj.qty)
        if modifiedPrice == gPrice:
            print "Mod(previousObj)",lineSplitList[0], gSellPriceDictionary[lPrevPriceObj.price].totalQty,lPrevPriceObj.qty,modifiedQty
        if gVerificationOrderFoundFlag == True and modifiedPrice > gVerificationOrdPrice:
            del gOrderDictionary[exOrderId]
            return;

#deleted for Priority change
        if gVerificationOrderFoundFlag == True :
            if modifiedPrice == gVerificationOrdPrice:
                if lPrevPriceObj.price != modifiedPrice or (lPrevPriceObj.price == modifiedPrice and lPrevPriceObj.qty < modifiedQty):
                    del gOrderDictionary[exOrderId]
                    return

        if modifiedPrice not in gSellPriceDictionary:
            lPriceObj = Price(modifiedPrice)
            lPriceObj.totalQty = 0;
            gSellPriceDictionary[modifiedPrice] = lPriceObj   #gSellPriceDictionary{9014: obj}
    
        gSellPriceDictionary[modifiedPrice].totalQty = gSellPriceDictionary[modifiedPrice].totalQty + modifiedQty;
        if modifiedPrice == gPrice:
            print "Mod:Sell", lineSplitList[0], gSellPriceDictionary[modifiedPrice].totalQty,lPrevPriceObj.qty,modifiedQty

    gOrderDictionary[exOrderId].price = modifiedPrice
    gOrderDictionary[exOrderId].qty = modifiedQty
    

#1267324,1575258571675811,1,1,1267324,T,1259745571666071199,1000000000346727,1000000000405031,9014,44700,5
def tradedOrder(line):
    global gBuyPriceDictionary
    global gSellPriceDictionary
    global gOrderDictionary
    global gSide
    global gVerificationOrdPrice
    global gVerificationOrderFoundFlag
    global gInputExOrderId
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
        if gInputExOrderId == lBuyExOrderId:
            if lBuyObj.qty == 0:
                print "Provied Ex-order-id already traded."
                sys.exit()
        
         
    if lSellExOrderId in gOrderDictionary:
        lSellObj = gOrderDictionary[lSellExOrderId]
        gSellPriceDictionary[lSellObj.price].totalQty = gSellPriceDictionary[lSellObj.price].totalQty - tradedQty
        if lSellObj.price == gPrice:
            print "Trade:Sell", lineSplitList[0], gSellPriceDictionary[lSellObj.price].totalQty,tradedQty, lBuyExOrderId
        lSellObj.qty = lSellObj.qty - tradedQty
        gOrderDictionary[lSellExOrderId] = lSellObj
        if gInputExOrderId == lSellExOrderId and lSellObj.qty == 0:
            print "Provied Ex-order-id already traded."
            sys.exit()
        
    

#1235164,1575258561501225,1,1,1235164,X,1259745561473796814,1000000000086416,9014,S,45560,7
def canceledOrder(line):
    global gBuyPriceDictionary
    global gSellPriceDictionary
    global gOrderDictionary
    global gSide
    global gVerificationOrdPrice
    global gVerificationOrderFoundFlag
    global gInputExOrderId
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

    if gInputExOrderId == exOrderId:
        CalculationOfNeedToTradeForAOrder()
        print line
        print "Provided Ex-Order-id is cancelled"        
        sys.exit()

    
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
'''
    if lSide == 'B':
	if lCanPrice not in gBuyPriceDictionary:
		return
    	gBuyPriceDictionary[lCanPrice].totalQty = gBuyPriceDictionary[lCanPrice].totalQty - lCanQty
        return
    else:
	if lCanPrice not in gSellPriceDictionary:
		return
    	gSellPriceDictionary[lCanPrice].totalQty = gSellPriceDictionary[lCanPrice].totalQty - lCanQty
        return
'''
        
                
def totalQtyBeforeMe(file):
  global gepoch
  with open(file, "r") as tokenfile:
    for line in tokenfile:
      order_type = line.split(",")[5]
      time1 = int(line.split(",")[1])
      l =  datetime.datetime.fromtimestamp(int(time1)/1000000).strftime('%c')
      if order_type == 'N':
        newOrder(line)
        
      if order_type == 'M':
        modifyOrder(line)

      if order_type == 'T':
        tradedOrder(line)

      if order_type == 'X':
        canceledOrder(line)

def main():
    global gVerificationOrderFoundFlag
    global gepoch
    global gtime
    print fn
    totalQtyBeforeMe(fn)
    
    
    if gVerificationOrderFoundFlag == False:
        print "Order not found, plz provide correct exchange-order-id and Token combination."
        sys.exit(0)
    
    CalculationOfNeedToTradeForAOrder();
    #print("\nExecution Time %s Seconds ---" % (time.time() - start_time))
    
    
if __name__== "__main__":
  main()


