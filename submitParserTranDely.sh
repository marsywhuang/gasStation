#!/bin/bash

for idxItem in "18" "19" "20" "21" "22" "23" "24" "25" "26" "27" "28" "29" "30" "31"
do
  spark-submit parserTranDelt_v1.py "2018-01-"$idxItem
done
