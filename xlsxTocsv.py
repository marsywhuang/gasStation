
//
inputDirectory = "/home/mywh/data/能源局各縣市汽車加油站汽柴油銷售統計表/"
inputFileExtension = "xlsx"
//
outputDirectory = "/home/mywh/data/output"
// patterns
strPattern = ["- 各縣市加油站汽柴油銷售分析表",
              "加油站汽柴油銷售",
              "加油站汽柴油銷售表",
              "各縣市加油站汽柴油售分析",
              "各縣市加油站汽柴油銷售分析表",
              "各縣市加油站汽柴油銷售統計表",
              "各縣市加油站銷售分析表",
              "各縣市汽車加油站汽柴油銷售統計表",
              "各縣市汽柴油銷售分析表",
              "各縣市汽柴油銷售表",
              "-各縣市售油量分析表",
              "-各縣市售油量彙整表",
              "汽柴油銷售",
              "汽柴油銷售分析",
              "汽柴油銷售分析表",
              "汽柴油銷售表",
              "汽柴油銷售量統計"]


//
cmd = "xlsx2csv"
cmdArg = "--all"

import os

for root, dirs, files in os.walk(inputDirectory):  
    for filename in files:
        if fname.endswith(inputFileExtension):
            print(filename)
            for idxItem in strPattern:
                re.search(idxItem, filename.split(".")[0])
                
                
for fileName in *.xlsx
do
  tmp=${fileName%.xlsx}
  xlsx2csv $tmp.xlsx --all > $tmp.csv
done
