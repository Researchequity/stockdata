#!/usr/bin/env python
import sys,os
import datetime
if sys.version_info[0] == 2:
    import ConfigParser as configparser
else:
    import configparser

config = configparser.ConfigParser()
config.optionxform = lambda option: option  # to change uppecase
config.readfp(open(r'config2.ini'))


string_val = config.get('SL-TOKEN', 'VALUE')
print "Your Existing Token:", string_val

add = raw_input("Press 1 if you want to add new token:\nPress 2 if you want to edit existing token: \n") 
if int(add) == 1:
  token = raw_input("Enter your token : ") 
  qty = raw_input("Enter your qty : ") 
  price = raw_input("Enter your price range i.e 100-200: ") 
  ov = raw_input("Enter your order value: ") 
  ov = int(ov)*100

  string_val = token
  print string_val
  config.set('SL-TOKEN', 'VALUE', string_val)

  config.add_section(token)
  config.set(token, 'QTY', qty)
  config.set(token, 'PRICE', price)
  config.set(token, 'ORDER-VALUE', ov)

if int(add) == 2:
  token = raw_input("Enter your token : ") 
  q = config.get(token, 'QTY')
  p = config.get(token, 'PRICE')
  v = config.get(token, 'ORDER-VALUE')
  print "=========== Existing Values ==========="
  print "TOKEN=", token
  print "QTY=", q
  print "PRICE=", p
  print "ORDER-VALUE=", int(v)/100
  print "======================================="
  c = raw_input("Do you want modify qty: ")
  if c == 'y' or c == 'Y':
   qty = raw_input("Enter your qty : ") 
   config.set(token, 'QTY', qty)
   
  c = raw_input("Do you want modify price range: ")
  if c == 'y' or c == 'Y':
    price = raw_input("Enter your price range i.e 100-200: ") 
    config.set(token, 'PRICE', price)
    
  c = raw_input("Do you want modify order value: ")
  if c == 'y' or c == 'Y':
    ov = raw_input("Enter your order value: ") 
    ov = int(ov)*100
    config.set(token, 'ORDER-VALUE', ov)
  string_val = token
  config.set('SL-TOKEN', 'VALUE', string_val)

  #config.add_section(token)
  

with open('config2.ini', 'w') as configfile:
    config.write(configfile)

