#!/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:/root/bin
export DISPLAY=:0.0
cd /home/dwh/
screen -dmS TBT_FNO_STREAM_WISE
echo "Screen Created."
sleep 1
screen -S TBT_FNO_STREAM_WISE -X screen -t Stream123 bash -c "cd tbt-fno-str123; LD_PRELOAD=libvma.so VMA_RX_POLL=-1 ./tbt_dumper_09022021.bin --ex=all --internval=90; exec sh"
sleep 1
screen -S TBT_FNO_STREAM_WISE -X screen -t Stream456 bash -c "cd tbt-fno-str456; LD_PRELOAD=libvma.so VMA_RX_POLL=-1 ./tbt_dumper_09022021.bin --ex=all --internval=90; exec sh"
sleep 1
screen -S TBT_FNO_STREAM_WISE -X screen -t Stream789 bash -c "cd tbt-fno-str789; LD_PRELOAD=libvma.so VMA_RX_POLL=-1 ./tbt_dumper_09022021.bin --ex=all --internval=90; exec sh"
sleep 1
screen -S TBT_FNO_STREAM_WISE -X screen -t Stream101112 bash -c "cd tbt-fno-str101112; LD_PRELOAD=libvma.so VMA_RX_POLL=-1 ./tbt_dumper_09022021.bin --ex=all --internval=90; exec sh"
sleep 1
screen -S TBT_FNO_STREAM_WISE -X screen -t Stream121415 bash -c "cd tbt-fno-str131415; LD_PRELOAD=libvma.so VMA_RX_POLL=-1 ./tbt_dumper_09022021.bin --ex=all --internval=90; exec sh"
sleep 1
