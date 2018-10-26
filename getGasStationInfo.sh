#!/bin/sh

# 區域
for r in 5 6 7 8
do
  # 縣市
  for c in 1 2 3 4 5 6 7 8 9
  do
    # 頁次
    for p in 1 2 3 4 5 6 7 8 9
    do
      # 擷取網頁內容
      curl http://www.fpcc.com.tw/tc/station_full.php?region=$r\&county=$c\&ctype=0\&page=$p > tmpGasStationInfo.txt
      # 判斷是否有資料
      cat -n tmpGasStationInfo.txt | grep table | awk 'BEGIN {prev=0; curr=0} {curr=$1; if (prev !=0 ) print (curr-prev); prev=curr}'
      #
    done
  done
done
