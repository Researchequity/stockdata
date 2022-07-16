#!/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:/root/bin
export DISPLAY=:0.0
#if [ "$#" -ne 1 ]; then
#  echo "Usage: $0 LOGFILENAME" >&2
#  exit 1
#fi

FOL=`date +"%d%m%Y"`
file='DUMP_'`date +"%Y%m%d_*"`
echo $file
mkdir -p ../data/$FOL

head -n 1000000 /home/dwh/tbt-ncash-str1/$file | grep ",1,1," > ../data/$FOL/STR1.txt
head -n 1000000 /home/dwh/tbt-ncash-str2/$file | grep ",2,1," > ../data/$FOL/STR2.txt
head -n 1000000 /home/dwh/tbt-ncash-str3/$file | grep ",3,1," > ../data/$FOL/STR3.txt
head -n 1000000 /home/dwh/tbt-ncash-str4/$file | grep ",4,1," > ../data/$FOL/STR4.txt

#tar czf STR.tar.gz STR*
