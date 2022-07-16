import sys
import socket

def updatePreopentime(filename, preopentime):
        #preopentime=sys.argv[1];


        print filename
        readfp=open(filename, "r")
        storelines = readfp.readlines()
        readfp.close()

        writefp=open(filename, "w");

        for l in storelines:
                if l.find("[int]output_ncash_preopen_order_delay_in_ms[0]") == 0:
                        preopenline = "[int]output_ncash_preopen_order_delay_in_ms[0]="+preopentime+";"
                        writefp.write(preopenline+"\n");
                else:   
                        writefp.write(l);

        writefp.close();

preopentime=sys.argv[1]
hostname = socket.gethostname()
filename="/home/blaze/indr240/newblaze1/configurations/"+hostname+".tcf";
updatePreopentime(filename, preopentime);

preopentime=sys.argv[2]
filename="/home/blaze/indr240/newblaze2/configurations/"+hostname+".tcf";
updatePreopentime(filename, preopentime);

#preopentime=sys.argv[3]
#filename="/home/blaze/indr240/newblaze3/configurations/"+hostname+".tcf";
#updatePreopentime(filename, preopentime);

#preopentime=sys.argv[4]
#filename="/home/blaze/indr240/newblaze4/configurations/"+hostname+".tcf";
#updatePreopentime(filename, preopentime);
