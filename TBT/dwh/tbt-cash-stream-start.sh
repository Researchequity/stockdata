#!/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:/root/bin
export DISPLAY=:0.0
cd /home/dwh/
screen -dmS TBT_CASH_STREAM_WISE
echo "Screen Created."
screen -S TBT_CASH_STREAM_WISE -X screen -t Stream1 bash -c "cd tbt-ncash-str1; ./tbt_dumper.bin --ex=all --internval=90; exec sh"
sleep 1
screen -S TBT_CASH_STREAM_WISE -X screen -t Stream2 bash -c "cd tbt-ncash-str2; ./tbt_dumper.bin --ex=all --internval=90; exec sh"
sleep 1
screen -S TBT_CASH_STREAM_WISE -X screen -t Stream3 bash -c "cd tbt-ncash-str3; ./tbt_dumper.bin --ex=all --internval=90; exec sh"
sleep 1
screen -S TBT_CASH_STREAM_WISE -X screen -t Stream4 bash -c "cd tbt-ncash-str4; ./tbt_dumper.bin --ex=all --internval=90; exec sh"

