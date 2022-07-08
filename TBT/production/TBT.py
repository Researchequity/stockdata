import time
import struct
import socket
import sys

group = '239.60.60.44'
MYPORT  = 16644
MYTTL = 0

addrinfo = socket.getaddrinfo(group, None)[0]
s = socket.socket(addrinfo[0], socket.SOCK_DGRAM)

# Look up multicast group address in name server and find out IP version
addrinfo = socket.getaddrinfo(group, None)[0]

# Create a socket
s = socket.socket(addrinfo[0], socket.SOCK_DGRAM)

# Allow multiple copies of this program on one machine
# (not strictly needed)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

# Bind it to the port
s.bind(('', MYPORT))

group_bin = socket.inet_pton(addrinfo[0], addrinfo[4][0])
# Join group
mreq = group_bin + struct.pack('=I', socket.INADDR_ANY)
s.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

while True:
    data, sender = s.recvfrom(1500)
    while data[-1:] == '\0': data = data[:-1] # Strip trailing \0's
    #print (str(sender) + '  ' + repr(data))
    a_binary_string =  repr(data)
    with open('fileName_n.csv', 'a') as f:
        f.write(a_binary_string)
        f.write("\n")

    #
    # binary_values = a_binary_string.split('\\')
    # print(binary_values)
    # for binary_value in binary_values:
    #     try:
    #         an_integer = int(binary_value, 2)
    #         ascii_character = chr(an_integer)
    #         print(ascii_character)
    #     except:
    #         continue




