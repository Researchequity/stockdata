rm -rf /home/dwh/tbt-ncash-str1/DUMP_*.DAT
rm -rf /home/dwh/tbt-ncash-str2/DUMP_*.DAT
rm -rf /home/dwh/tbt-ncash-str3/DUMP_*.DAT
rm -rf /home/dwh/tbt-ncash-str4/DUMP_*.DAT

rm -rf /home/dwh/tbt-ncash-str12/DUMP_*.DAT
rm -rf /home/dwh/tbt-ncash-str34/DUMP_*.DAT

rm -rf /home/dwh/tbt-fno-str1/DUMP_*.DAT
rm -rf /home/dwh/tbt-fno-str2/DUMP_*.DAT
rm -rf /home/dwh/tbt-fno-str3/DUMP_*.DAT
rm -rf /home/dwh/tbt-fno-str4/DUMP_*.DAT
rm -rf /home/dwh/tbt-fno-str5/DUMP_*.DAT
rm -rf /home/dwh/tbt-fno-str6/DUMP_*.DAT

rm -rf /home/dwh/tbt-fno-str123/DUMP_*.DAT
rm -rf /home/dwh/tbt-fno-str456/DUMP_*.DAT
rm -rf /home/dwh/tbt-fno-str789/DUMP_*.DAT
rm -rf /home/dwh/tbt-fno-str101112/DUMP_*.DAT
rm -rf /home/dwh/tbt-fno-str131415/DUMP_*.DAT

killall tbt_dumper.bin
screen -X -S TBT_CASH_STREAM_WISE quit
screen -X -S TBT_FNO_STREAM_WISE quit

