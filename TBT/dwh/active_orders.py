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
gActiveBuyOrderDictionary = {}
gActiveSellOrderDictionary = {}
gActiveOrderDictionary = {}
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
new_order = ''
index = 1

RED   = "\033[1;31m"  
BLUE  = "\033[1;34m"
CYAN  = "\033[1;36m"
GREEN = "\033[0;32m"
RESET = "\033[0;0m"
BOLD    = "\033[;1m"
REVERSE = "\033[;7m"

if len(sys.argv) == 3:
  file_name1 = sys.argv[1]
  gToken = sys.argv[2]
#   gVerificationQty = sys.argv[3]
#   gVerificationOrdPrice1=sys.argv[4]
#   gVerificationOrdPrice2=sys.argv[5]
#   gStartTime = sys.argv[6]
#   gEndTime = sys.argv[7]
 

gVerificationQty = raw_input("\nPlease Provied Quantity : ")
time_interval = raw_input("\nPlease Provied Time Interval(e.g. 9:00:00-9:25:00): ")
gStartTime =  time_interval.split("-")[0]
gEndTime =  time_interval.split("-")[1]

price_range = raw_input("\nPlease Provied Price Range(e.g. 10-10000): ")
gVerificationOrdPrice1 = price_range.split("-")[0]
gVerificationOrdPrice2 = price_range.split("-")[1]
  
a=","+gToken+","
fn=gToken+".txt"

today = date.today()
d1 = today.strftime("%Y%m%d")

file_name = "tbt-ncash-str"+ str(file_name1) + "/" + "DUMP_" + d1 +"_*"

#print file_name
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
    self.order_qty = 0
    
class Price():
  def __init__(self, orderPrice):
    self.orderPrice = orderPrice
    self.totalQty = 0
#    self.side = ''
#     self.buyDic = {}
#     self.sellDic = {}


def orderDetail(orderID):
    with open(fn, "r") as tokenfile:
        for line in tokenfile:
            lineSplitList = line.rstrip().split(",")
            order_type = line.split(",")[5]
            exOrderId = line.split(",")[7]
            exOrderId2 = line.split(",")[8]
            time1 = int(line.split(",")[1])
            l =  (datetime.datetime.fromtimestamp(int(time1)/1000000).strftime('%c')).split(" ")[4]
            if exOrderId == orderID or exOrderId2 == orderID:
                print l,",",lineSplitList[5],",",lineSplitList[7],",",lineSplitList[8],",",lineSplitList[9],",",lineSplitList[10],",",lineSplitList[11]

def orderDetail(orderID):
    trade_count=0
    trade_price=0
    with open(fn, "r") as tokenfile:
        for line in tokenfile:
            lineSplitList = line.rstrip().split(",")
            order_type = line.split(",")[5]
            exOrderId = line.split(",")[7]
            exOrderId2 = line.split(",")[8]
            time1 = int(line.split(",")[1])
            l =  (datetime.datetime.fromtimestamp(int(time1)/1000000).strftime('%c')).split(" ")[3]
            if exOrderId == orderID or exOrderId2 == orderID:
                print l,",",lineSplitList[5],",",lineSplitList[7],",",lineSplitList[8],",",lineSplitList[9],",",lineSplitList[10],",",lineSplitList[11]
                if str(lineSplitList[5]) == 'T':
                    trade_price = trade_price + int(lineSplitList[10])
                    trade_count = trade_count+1
    if (trade_count != 0):
        print "\nTrade Count: ", trade_count
        print "Average Trade Price: ", trade_price/trade_count

def average_trade_price(orderID):
    trade_count=0
    trade_price=0
    with open(fn, "r") as tokenfile:
        for line in tokenfile:
            lineSplitList = line.rstrip().split(",")
            exOrderId = line.split(",")[7]
            exOrderId2 = line.split(",")[8]
            if exOrderId == orderID or exOrderId2 == orderID:
                if str(lineSplitList[5]) == 'T':
                    trade_price = trade_price + int(lineSplitList[10])
                    trade_count = trade_count+1
    if (trade_count != 0):
        return trade_price/trade_count


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
    global gDepth
    global gActiveBuyOrderDictionary
    global gActiveSellOrderDictionary
    global gActiveOrderDictionary
    
    fp = open("token_detail.csv", "w") 
    
    totalQty = 0
    print_b_s_dict={}
    buy_list = []
    sell_list = []
    order_dick = {}
    buy_list_dick = {}
    sell_list_dick = {}
    fp = open("active_orders.txt", "w")
    l=1
    for orderID in sorted(gActiveBuyOrderDictionary.keys()):
        buy_list.append(str(l)+", "+str(orderID)+", "+str(gActiveBuyOrderDictionary[orderID].price)+", "+str(gActiveBuyOrderDictionary[orderID].order_qty)+", "+str(gActiveBuyOrderDictionary[orderID].side )+", ")
        order_dick[l] = orderID
        l = l+1
    for orderID in sorted(gActiveSellOrderDictionary.keys()):
        sell_list.append(str(l)+", "+str(orderID)+", "+str(gActiveSellOrderDictionary[orderID].price)+", "+str(gActiveSellOrderDictionary[orderID].order_qty)+", "+str(gActiveSellOrderDictionary[orderID].side ))  
        order_dick[l] = orderID
        l = l+1

    l=1
    for orderID in sorted(gActiveOrderDictionary.keys()):
        if  str(gActiveOrderDictionary[orderID].side) == 'S':
            sys.stdout.write(BOLD)
            sys.stdout.write(RED)
            atp = average_trade_price(orderID)
            print str(l)+", "+str(orderID)+", "+str(gActiveOrderDictionary[orderID].price)+", "+str(gActiveOrderDictionary[orderID].order_qty)+", "+str(gActiveOrderDictionary[orderID].side+", "+str(atp) )
            output = str(l)+", "+str(orderID)+", "+str(gActiveOrderDictionary[orderID].price)+", "+str(gActiveOrderDictionary[orderID].order_qty)+", "+str(gActiveOrderDictionary[orderID].side+", "+str(atp))
            fp.write(output)
        else:
            sys.stdout.write(BOLD)
            sys.stdout.write(GREEN)
            atp = average_trade_price(orderID)
            print str(l)+", "+str(orderID)+", "+str(gActiveOrderDictionary[orderID].price)+", "+str(gActiveOrderDictionary[orderID].order_qty)+", "+str(gActiveOrderDictionary[orderID].side+", "+str(atp))
            output = str(l)+", "+str(orderID)+", "+str(gActiveOrderDictionary[orderID].price)+", "+str(gActiveOrderDictionary[orderID].order_qty)+", "+str(gActiveOrderDictionary[orderID].side+", "+str(atp) )
            fp.write(output)
            
        order_dick[l] = orderID
        l = l+1    
    fp.close()
    sys.stdout.write(RESET)
    
    if len(buy_list) < len(sell_list):
        for i in range(len(sell_list) - len(buy_list)):
            sell_list[len(buy_list)+i] = "\t\t\t\t" + sell_list[len(buy_list)+i]
    a = []       
    a.append(buy_list)
    a.append(sell_list)
        
    #print '\n'.join(['\t'.join([str(x[i]) if len(x) > i else '' for x in a]) for i in range(len(max(a)))])

    while 1:
        order_index = raw_input("\nPress 0 For Exit OR Press Order Index: ")
        if int(order_index) == 0:
            break
        order_id = order_dick[int(order_index)]
        
        orderDetail(order_id)
                
def newOrder(line):
    global gBuyPriceDictionary
    global gSellPriceDictionary
    global gOrderDictionary
    global gSide
    global gVerificationOrdPrice1
    global gVerificationOrdPrice2;
    global gVerificationOrderFoundFlag
    global gToken
    global gPrice
    global gBuyPendingQty
    global gSellPendingQty
    global gActiveBuyOrderDictionary
    global gActiveSellOrderDictionary
    global gActiveOrderDictionary
    global gVerificationQty
    global index
    
    lineSplitList = line.split(",")
    exOrderId = lineSplitList[7]
    orderPrice = int(lineSplitList[10])
    orderQty = int(lineSplitList[11])
    if gToken != lineSplitList[8]:
        return
    
    neworder_obj = OrderElement(exOrderId)
    neworder_obj.price = int(lineSplitList[10])
    neworder_obj.qty = int(lineSplitList[11])
    neworder_obj.order_qty = int(lineSplitList[11])
    neworder_obj.side = lineSplitList[9]
    
    if neworder_obj.side == 'B':
        if orderPrice not in gBuyPriceDictionary:
            lPriceObj = Price(orderPrice)
            lPriceObj.totalQty = 0;
            gBuyPriceDictionary[orderPrice] = lPriceObj   #gBuyPriceDictionary{9014: obj}
        
        gBuyPendingQty = gBuyPendingQty + neworder_obj.qty 
        gOrderDictionary[exOrderId] = neworder_obj
        gBuyPriceDictionary[orderPrice].totalQty = gBuyPriceDictionary[orderPrice].totalQty + neworder_obj.qty;
        #print gVerificationQty, neworder_obj.order_qty
        if orderPrice in gSellPriceDictionary and gSellPriceDictionary[neworder_obj.price].totalQty != 0 and int(gVerificationQty) <= int(neworder_obj.order_qty) and int(gVerificationOrdPrice1) <= neworder_obj.price and int(gVerificationOrdPrice2) >= neworder_obj.price :
            gActiveBuyOrderDictionary[exOrderId] = neworder_obj
            gActiveOrderDictionary[exOrderId] = neworder_obj
            #print index, exOrderId, gActiveBuyOrderDictionary[exOrderId].price, gActiveBuyOrderDictionary[exOrderId].order_qty, gActiveBuyOrderDictionary[exOrderId].side
            index = index+1

    else:
        if orderPrice not in gSellPriceDictionary:
            lPriceObj = Price(orderPrice)
            lPriceObj.totalQty = 0;
            gSellPriceDictionary[orderPrice] = lPriceObj   #gSellPriceDictionary{9014: obj}
        
        gSellPendingQty = gSellPendingQty + neworder_obj.qty 
        gOrderDictionary[exOrderId] = neworder_obj
        gSellPriceDictionary[orderPrice].totalQty = gSellPriceDictionary[orderPrice].totalQty + neworder_obj.qty;
        
        if orderPrice in gBuyPriceDictionary and gBuyPriceDictionary[neworder_obj.price].totalQty != 0 and int(gVerificationQty) <= int(neworder_obj.order_qty)and int(gVerificationOrdPrice1) <= neworder_obj.price and int(gVerificationOrdPrice2) >= neworder_obj.price :
            gActiveSellOrderDictionary[exOrderId] = neworder_obj
            gActiveOrderDictionary[exOrderId] = neworder_obj
            #print str(index)+", "+str(exOrderId)+", "+str(gActiveSellOrderDictionary[exOrderId].price)+", "+str(gActiveSellOrderDictionary[exOrderId].order_qty)+", "+str(gActiveSellOrderDictionary[exOrderId].side )  
            index = index+1
    
def modifyOrder(line):
    global gBuyPriceDictionary
    global gSellPriceDictionary
    global gOrderDictionary
    global gSide
    global gVerificationOrdPrice1
    global gVerificationOrderFoundFlag
    global gToken
    global gPrice
    global gBuyPendingQty
    global gSellPendingQty
    
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
    
    if side == 'B':
        if exOrderId not in gOrderDictionary:
            gOrderDictionary[exOrderId] = modifiedObj;
            if modifiedPrice not in gBuyPriceDictionary:
                lPriceObj = Price(modifiedPrice)
                lPriceObj.totalQty = 0;
                gBuyPriceDictionary[modifiedPrice] = lPriceObj
            gBuyPendingQty = gBuyPendingQty + modifiedQty
            gBuyPriceDictionary[modifiedPrice].totalQty = gBuyPriceDictionary[modifiedPrice].totalQty + modifiedObj.qty;
            #print "Buy Quantity Added: ", modifiedObj.qty, exOrderId
    if side == 'S':
        if exOrderId not in gOrderDictionary:
            gOrderDictionary[exOrderId] = modifiedObj;
            if modifiedPrice not in gSellPriceDictionary:
                lPriceObj = Price(modifiedPrice)
                lPriceObj.totalQty = 0;
                gSellPriceDictionary[modifiedPrice] = lPriceObj
            gSellPendingQty = gSellPendingQty + modifiedQty
            gSellPriceDictionary[modifiedPrice].totalQty = gSellPriceDictionary[modifiedPrice].totalQty + modifiedObj.qty;
            #print "Sell Quantity Added: ", modifiedObj.qty

    lPrevPriceObj = gOrderDictionary[exOrderId];
            
    if side == 'B':
        gBuyPriceDictionary[lPrevPriceObj.price].totalQty = gBuyPriceDictionary[lPrevPriceObj.price].totalQty - int(lPrevPriceObj.qty)
        
        if modifiedPrice not in gBuyPriceDictionary:
            lPriceObj = Price(modifiedPrice)
            lPriceObj.totalQty = 0;
            gBuyPriceDictionary[modifiedPrice] = lPriceObj   #gBuyPriceDictionary{9014: obj}
            
        gBuyPendingQty = gBuyPendingQty - int(lPrevPriceObj.qty) + modifiedQty
        gBuyPriceDictionary[modifiedPrice].totalQty = gBuyPriceDictionary[modifiedPrice].totalQty + modifiedQty;
    else:
        gSellPriceDictionary[lPrevPriceObj.price].totalQty = gSellPriceDictionary[lPrevPriceObj.price].totalQty - int(lPrevPriceObj.qty)

        if modifiedPrice not in gSellPriceDictionary:
            lPriceObj = Price(modifiedPrice)
            lPriceObj.totalQty = 0;
            gSellPriceDictionary[modifiedPrice] = lPriceObj   #gSellPriceDictionary{9014: obj}
    
        gSellPendingQty = gSellPendingQty - int(lPrevPriceObj.qty) + modifiedQty
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
    global gBuyPendingQty
    global gSellPendingQty
    global gTotalTradedQuantity
    lineSplitList = line.rstrip().split(",")
    if gToken != lineSplitList[9]:
        return

    
    lBuyExOrderId = lineSplitList[7]
    lSellExOrderId = lineSplitList[8]
    tradedPrice = int(lineSplitList[10])
    tradedQty = int(lineSplitList[11])

    gTotalTradedQuantity = gTotalTradedQuantity + tradedPrice*tradedQty
    if lBuyExOrderId in gOrderDictionary:
        lBuyObj = gOrderDictionary[lBuyExOrderId]
        gBuyPriceDictionary[lBuyObj.price].totalQty = gBuyPriceDictionary[lBuyObj.price].totalQty - tradedQty
        if lBuyObj.price == gPrice:
            print "Trade:Buy", lineSplitList[0], gBuyPriceDictionary[lBuyObj.price].totalQty,tradedQty, lBuyExOrderId
        lBuyObj.qty = lBuyObj.qty - tradedQty
        gOrderDictionary[lBuyExOrderId] = lBuyObj
        gBuyPendingQty = gBuyPendingQty - tradedQty
         
    if lSellExOrderId in gOrderDictionary:
        lSellObj = gOrderDictionary[lSellExOrderId]
        gSellPriceDictionary[lSellObj.price].totalQty = gSellPriceDictionary[lSellObj.price].totalQty - tradedQty
        if lSellObj.price == gPrice:
            print "Trade:Sell", lineSplitList[0], gSellPriceDictionary[lSellObj.price].totalQty,tradedQty, lBuyExOrderId
        lSellObj.qty = lSellObj.qty - tradedQty
        gOrderDictionary[lSellExOrderId] = lSellObj
        gSellPendingQty = gSellPendingQty - tradedQty

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
    global gBuyPendingQty
    global gSellPendingQty
    global gActiveBuyOrderDictionary
    global gActiveSellOrderDictionary
    global gActiveOrderDictionary
    
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
            gBuyPendingQty = gBuyPendingQty - lCanQty
            if exOrderId in gActiveBuyOrderDictionary:
                del gActiveBuyOrderDictionary[exOrderId]
            if lBuyObj.price == gPrice:
                print "Cancel:Buy", lineSplitList[0], gBuyPriceDictionary[lBuyObj.price].totalQty,lCanQty, exOrderId
    if lSide == 'S':
        if exOrderId in gOrderDictionary:
            lSellObj = gOrderDictionary[exOrderId]
            gSellPriceDictionary[lSellObj.price].totalQty = gSellPriceDictionary[lSellObj.price].totalQty - lCanQty 
            gSellPendingQty = gSellPendingQty - lCanQty     
            if exOrderId in gActiveSellOrderDictionary:
                del gActiveSellOrderDictionary[exOrderId]            
            if lSellObj.price == gPrice:
                print "Cancel:Sell", lineSplitList[0], gSellPriceDictionary[lSellObj.price].totalQty,lCanQty, exOrderId      
    del gOrderDictionary[exOrderId]
    if exOrderId in gActiveOrderDictionary:
        del gActiveOrderDictionary[exOrderId]
        
                
def totalQtyBeforeMe(file):
    global gepochStartTime
    global gepochEndTime
    global new_order
    with open(file, "r") as tokenfile:
        for line in tokenfile:
            order_type = line.split(",")[5]
            time1 = int(line.split(",")[1])
            l =  datetime.datetime.fromtimestamp(int(time1)/1000000).strftime('%c')
#             if time1 < gepochStartTime:
#                 continue
#             if time1 > gepochEndTime:
#                 print l, gepochEndTime, "Time Up"
#                 #CalculationOfNeedToTradeForAOrder();
#                 sys.exit()
                
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
    global gepochStartTime
    global gStartTime    
    global gepochEndTime
    global gEndTime
    global new_order
    print fn
    today = date.today()
    d1 = today.strftime("%d:%m:%Y")
    
    date_time = str(d1 +" "+ gStartTime)
    pattern = '%d:%m:%Y %H:%M:%S'
    gepochStartTime = int(time.mktime(time.strptime(date_time, pattern)))*1000000
    
    date_time = str(d1 +" "+ gEndTime)
    gepochEndTime = int(time.mktime(time.strptime(date_time, pattern)))*1000000
    
    totalQtyBeforeMe(fn)
    CalculationOfNeedToTradeForAOrder();
    #print("\nExecution Time %s Seconds ---" % (time.time() - start_time))
    
    
if __name__== "__main__":
  main()


