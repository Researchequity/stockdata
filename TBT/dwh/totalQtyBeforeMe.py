#!/usr/bin/env python
import sys,os
from __builtin__ import True
gOrderDictionary = {}
gVerificationOrdPrice=0;
gVerificationOrderFoundFlag=False;
gInputExOrderId=0;
gSide=''

gBuyPriceDictionary = {} 
gSellPriceDictionary = {} 

if len(sys.argv) == 4:
  file_name = sys.argv[1]
  gInputExOrderId = sys.argv[2]
  token = sys.argv[3]

  

a=","+token+","
fn=token+".txt"
print a
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
    totalQty = 0

    if gSide == 'B':
        for priceKey in sorted(gBuyPriceDictionary.keys(), reverse=True):
            if int(gVerificationOrdPrice) <= priceKey:
                if gBuyPriceDictionary[priceKey].totalQty > 0:
                    print priceKey, gBuyPriceDictionary[priceKey].totalQty, totalQty
                    totalQty = totalQty + gBuyPriceDictionary[priceKey].totalQty
            else:
                break;
        
        print "BUY:At-Price:", gVerificationOrdPrice, "Remaing Qty :", totalQty, "Need to trade-qty:", (totalQty - gOrderDictionary[gInputExOrderId].qty) 
    
    counter = 0
    if gSide == 'S':
        for priceKey in sorted(gSellPriceDictionary.keys()):
#             print counter, priceKey, gVerificationOrdPrice, len(gSellPriceDictionary)
#             counter = counter + 1
            if int(gVerificationOrdPrice) >= priceKey:
                if gSellPriceDictionary[priceKey].totalQty > 0:
                    print priceKey, gSellPriceDictionary[priceKey].totalQty, totalQty
                    totalQty = totalQty + gSellPriceDictionary[priceKey].totalQty
            else:
                break;
                
        print "SELL:At-Price:", gVerificationOrdPrice, "Remaing Qty :", totalQty, "Need to trade-qty:", (totalQty - gOrderDictionary[gInputExOrderId].qty) 
    
    sys.exit()
    
    
def newOrder(line):
    global gBuyPriceDictionary
    global gSellPriceDictionary
    global gOrderDictionary
    global gSide
    global gVerificationOrdPrice
    global gVerificationOrderFoundFlag
    global gInputExOrderId

    lineSplitList = line.split(",")
    exOrderId = lineSplitList[7]
    orderPrice = int(lineSplitList[10])
        
#     if gVerificationOrderFoundFlag == True:
#         if gSide == 'B' and orderPrice < gVerificationOrdPrice:
#             return;
#         if gSide == 'S' and orderPrice > gVerificationOrdPrice:
#             return;
    
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
    
        
#     print gInputExOrderId, exOrderId
    if exOrderId == gInputExOrderId:
        gVerificationOrdPrice = orderPrice
        gSide = lineSplitList[9]
        gVerificationOrderFoundFlag = True; 
        print line



def modifyOrder(line):
    global gBuyPriceDictionary
    global gSellPriceDictionary
    global gOrderDictionary
    global gSide
    global gVerificationOrdPrice
    global gVerificationOrderFoundFlag
    global gInputExOrderId

    lineSplitList = line.rstrip().split(",")
    #print lineSplitList
    exOrderId = lineSplitList[7]
    modifiedPrice = int(lineSplitList[10])
    modifiedQty = int(lineSplitList[11])
    side = lineSplitList[9]
    
    if exOrderId not in gOrderDictionary:
        print "Mod order found should not calculate since it will come after provided order id:", exOrderId
        return;
    
    lPrevPriceObj = gOrderDictionary[exOrderId];
    
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
    global gVerificationOrdPrice
    global gVerificationOrderFoundFlag
    global gInputExOrderId

    lineSplitList = line.rstrip().split(",")
    lBuyExOrderId = lineSplitList[7]
    lSellExOrderId = lineSplitList[8]
    tradedPrice = int(lineSplitList[10])
    tradedQty = int(lineSplitList[11])

    #Handle buy order
    if lBuyExOrderId in gOrderDictionary:
        lBuyObj = gOrderDictionary[lBuyExOrderId]
        gBuyPriceDictionary[lBuyObj.price].totalQty = gBuyPriceDictionary[lBuyObj.price].totalQty - tradedQty
        lBuyObj.qty = lBuyObj.qty - tradedQty
        gOrderDictionary[lBuyExOrderId] = lBuyObj
        if gInputExOrderId == lBuyExOrderId and lBuyObj.qty == 0:
            print "Provied Ex-order-id already traded."
            sys.exit()
        
         
    if lSellExOrderId in gOrderDictionary:
        lSellObj = gOrderDictionary[lSellExOrderId]
        gSellPriceDictionary[lSellObj.price].totalQty = gSellPriceDictionary[lSellObj.price].totalQty - tradedQty
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

    lineSplitList = line.rstrip().split(",")
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

    
    del gOrderDictionary[exOrderId]
    if lSide == 'B':
        gBuyPriceDictionary[lCanPrice].totalQty = gBuyPriceDictionary[lCanPrice].totalQty - lCanQty
    else:
        gSellPriceDictionary[lCanPrice].totalQty = gSellPriceDictionary[lCanPrice].totalQty - lCanQty
    
        
                
def totalQtyBeforeMe(file):
  with open("9014.txt", "r") as tokenfile:
    for line in tokenfile:
      order_type = line.split(",")[5]
      
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
    
    totalQtyBeforeMe("fn")
    
    if gVerificationOrderFoundFlag == False:
        print "Order not found, plz provide correct exchange-order-id and Token combination."
        sys.exit(0)
        
    CalculationOfNeedToTradeForAOrder();
    
    
if __name__== "__main__":
  main()


