#!/bin/sh

# 年
for idxItemY in "2018"
do
  # 月
  for idxItemM in "01"
  do
    # 日
    for idxItemD in "20" "21" "22" "23" "24" "25" "26" "27" "28" "29" "30" "31"
    do
      # 顯示年月日的組合
      echo $idxItemY-$idxItemM-$idxItemD
      # 擷取特定區間記錄
      cat ../tran_master.csv | grep $idxItemY-$idxItemM-$idxItemD > tranMaster-$idxItemY$idxItemM$idxItemD.csv   
    done # idxItemD

  done # idxItemM

done # idxItemY
