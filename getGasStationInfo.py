#!/usr/bin/python
# -*- encoding: utf8-*-

import requests
from bs4 import BeautifulSoup
import geocoder

# 台亞加油站資料

gasStationUrl = "http://www.fpcc.com.tw/tc/station_full.php"

regionData = [5, 6, 7, 8]
countryData = [1, 2, 3, 4, 5, 6, 7, 8, 9]
pageData = [1, 2, 3, 4, 5, 6, 7, 8, 9]

# 變數宣告
tmpGasStationData = []
fileName = 'gasStationInfo.txt'

#
opFN = open(fileName, 'w')

# 區域
for idxRegion in regionData:
    # print ("Region", idxRegion)
    # 縣市
    for idxCountry in countryData:
        # print ("Country", idxCountry)
        # 頁數
        for idxPage in pageData:
            # print ("Page", idxPage)
            # 目標資料之URL
            tmpGasStationUrl = gasStationUrl + '?region=' + str(idxRegion) + '&county=' + str(idxCountry) + '&ctype=0&page=' + str(idxPage)

            # 擷取資料
            resultGasStationUrl = requests.get(tmpGasStationUrl)
            # 重新編碼
            resultGasStationUrl.encoding = 'uft-8'
            # 轉換成 BeautifulSoup 格式
            soupGasStation = BeautifulSoup(resultGasStationUrl.text)
            # 取出目標資料
            gasStationDataUrl = soupGasStation.findAll('div', {'id':'content_full'})[0].findAll('tr')

            #
            for idxRow in range(len(gasStationDataUrl)):
                #
                tmpGasStationData = []
                #
                tmpRecord = gasStationDataUrl[idxRow]
                tmpCells = tmpRecord.findAll("td")
                #
                for idxCell in range(len(tmpCells)):
                    # 文字表示
                    if tmpCells[idxCell].text != "":
                        # 去除不必要字元
                        tmpCell = str(tmpCells[idxCell].text).replace("\n", "").replace("\t", "").replace(" ", "")
                        # print (tmpCell)
                        # 取出經緯度
                        tmpGasStationData.append(tmpCell)
                        if  "maps" in str(tmpCells[idxCell].get_attribute_list):
                            tmpGeoLatLng = geocoder.arcgis(tmpCell)
                            # print (tmpGeoLatLng.lat, tmpGeoLatLng.lng)
                            tmpGasStationData.append(str(tmpGeoLatLng.lat))
                            tmpGasStationData.append(str(tmpGeoLatLng.lng))
                    # 非文字表示
                    else:
                        if  "check.png" in str(tmpCells[idxCell].get_attribute_list):
                            # print ("check")
                            #
                            tmpGasStationData.append('1')
                        else:
                            # print ("none")
                            #
                            tmpGasStationData.append('0')
                            
                #GasStationData.append(tmpGasStationData)
                
                if len(tmpGasStationData) != 0:
                    print(tmpGasStationData)
                    opFN.write(str(tmpGasStationData))

opfN.close()
