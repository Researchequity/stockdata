import sys
import socket

def updateAmotime(filename, amotime):
        #amotime=sys.argv[1];


        print filename
        readfp=open(filename, "r")
        storelines = readfp.readlines()
        readfp.close()

        writefp=open(filename, "w");

        for l in storelines:
                if l.find("[int]output_ncash_amo_order_delay_in_ms[0]") == 0:
                        preopenline = "[int]output_ncash_amo_order_delay_in_ms[0]="+amotime+";"
                        writefp.write(preopenline+"\n");
                else:   
                        writefp.write(l);

        writefp.close();

amotime=sys.argv[1]
hostname = socket.gethostname()
filename="/home/blaze/indr240/newblaze1/configurations/"+hostname+".tcf";
updateAmotime(filename, amotime);

amotime=sys.argv[2]
hostname = socket.gethostname()
filename="/home/blaze/indr240/newblaze2/configurations/"+hostname+".tcf";
updateAmotime(filename, amotime);


