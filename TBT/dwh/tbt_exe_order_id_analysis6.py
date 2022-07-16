#!/usr/bin/env python
import sys,os
import datetime
if sys.version_info[0] == 2:
    import ConfigParser as configparser
else:
    import configparser

config = configparser.ConfigParser()
config.readfp(open(r'config2.ini'))
priceRangeList = []
qtyList = []
orderValueList = []
tokenList = list(config.get('SL-TOKEN', 'VALUE').split(","))
print tokenList
for t in tokenList:
  priceRange = config.get(t, 'PRICE')
  qty = config.get(t, 'QTY')
  orderVakue = config.get(t, 'ORDER-VALUE')
  priceRangeList.append(priceRange)
  qtyList.append(qty)
  orderValueList.append(orderVakue)

#print tokenList, priceRangeList, qtyList, orderValueList
slOrderList=[]
stOrderID=0
buyQty=0
slDict = {}
demandList = {}
fullyTradedDic = {}
def tbt_analisys(file_name, sl_c, sl_c_f):
  fw = open("tbt_output.txt", "w")
  fw1 = open("remaining_quantity.txt", "w")
  fw2 = open("stoploss_and_fullytraded.txt", "w")
  g_ex_order_id = 0
  with open(file_name, "r") as my_file:
    for line in my_file:
      time1 = line.split(",")[1] 
      line=line.rstrip()
      st = line.split(",")
      if int(time1) > 1588756517000000: #1579059900000000: #1574739900000000:
        ex_order_id = line.split(",")[7] 
        ex_order_id_sell = line.split(",")[8] 
        if line.split(",")[5] == "N":
          if ex_order_id < g_ex_order_id :
            l =  datetime.datetime.fromtimestamp(int(time1)/1000000).strftime('%c')
            st[1] = str(time1) +"("+l+")" 
            st.remove(st[0])
            st.remove(st[1])
            st.remove(st[1])
            st.remove(st[1])
            st.remove(st[2])
            token1 = line.split(",")[8]
            ov = int(line.split(",")[10]) * int(line.split(",")[11])
            qty = int(line.split(",")[11])
            price = int(line.split(",")[10])
            if tokenList[0] != "none" or tokenList[0] != "None":
              try:
                index = tokenList.index(token1)
              except:
                continue
              minPrice = int(priceRangeList[index].split("-")[0])
              maxPrice = int(priceRangeList[index].split("-")[1])
              sl_c_f= sl_c_f+1
              if maxPrice == 0:
                maxPrice=price
              if token1 in tokenList and ov >= int(orderValueList[index]) and qty >= int(qtyList[index]) and price > minPrice and price <= maxPrice:
                stOrderID=ex_order_id
                sl_c= sl_c+1
                fw.write(' '.join([str(elem) for elem in st]) )
                fw.write('\n')
                fullyTradedDic[stOrderID] = ' '.join([str(elem) for elem in st])
                slOrderList.append(stOrderID)
                #slDict.update( {stOrderID : 0} )
                slDict[stOrderID] = 0
                v=[]
                v=[st[0],"   "+line.split(",")[9],"   "+token1,"  "+line.split(",")[10],"  "+line.split(",")[11]]
                demandList[stOrderID] = v
            else:
              fw.write(' '.join([str(elem) for elem in st]) )
              fw.write('\n')
          else:
            g_ex_order_id = ex_order_id

        if line.split(",")[5] == "T" and ex_order_id in slOrderList:
          value = slDict[ex_order_id] + int(line.split(",")[11])
          slDict[ex_order_id] = value
          fw.write(line)
        if line.split(",")[5] == "M" and ex_order_id in slOrderList:
          value = int(line.split(",")[11])
          if int(line.split(",")[10]) == int(demandList[ex_order_id][3]):
            slDict[ex_order_id] = 0
          demandList[ex_order_id][4] = value
        if line.split(",")[5] == "T" and ex_order_id_sell in slOrderList:
          value = slDict[ex_order_id_sell] + int(line.split(",")[11])
          slDict[ex_order_id_sell] = value
          fw.write(line)
        if line.split(",")[5] == "X" and ex_order_id_sell in demandList:
          del demandList[ex_order_id_sell]
        if line.split(",")[5] == "X" and ex_order_id in demandList:
          del demandList[ex_order_id]
  
  print "###################################################\n"
  fw1.write("    OrderID                     Time                          b/s  Token   price  qty  TTQ  Rmqty\n")
  print("    OrderID                     Time                          B/S  Token   Price  Qty  TTQ  Rmqty")
  for key in demandList:
    if int(demandList[key][4]) != int(slDict[key]):
      print slDict[key]
      fw1.write(key+" "+' '.join([str(elem) for elem in demandList[key]]) + "   "+str(slDict[key])+ "   "+str(int(demandList[key][4]) - int(slDict[key])) +"\n")
      print key+" "+' '.join([str(elem) for elem in demandList[key]]) + "   "+str(slDict[key])+ "   "+str(int(demandList[key][4]) - int(slDict[key]))
      #fullyTradedDic[key] = fullyTradedDic[key] + ": Not Fully Traded" 
      #fw2.write(fullyTradedDic[key] + "\n")
    else:
      fullyTradedDic[key] = fullyTradedDic[key] + ": Fully Traded" 
      fw2.write(fullyTradedDic[key] + "\n")
      #print fullyTradedDic[key] 
  print "\n###################################################\n"
  print "Total Number Of SL Order With Filtering: ", sl_c
  print "Total Number Of SL Order Without Filtering: ", sl_c_f
  print "\n###################################################"
      
if len(sys.argv) == 2:
  file_name = sys.argv[1]
  #print file_name

a=","+tokenList[0]+","#',2882,'
fn=tokenList[0]+".txt"
print a
#cmd='grep %s ../../tbt-ncash-str4/DUMP_20191211_073004.DAT'%a
cmd='grep %s '%a
cmd=cmd+'%s'%file_name
cmd=cmd+'>%s'%fn
print cmd
os.system(cmd)

sl_c=0
sl_c_f=0
#tbt_analisys(file_name, sl_c, sl_c_f)
tbt_analisys(fn, sl_c, sl_c_f)
#print fullyTradedDic
      






