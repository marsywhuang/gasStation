
//
inputDirectory = "/home/mywh/data/能源局各縣市汽車加油站汽柴油銷售統計表/"
inputFileExtension = "xlsx"

outputDirectory = "home/mywh/data/output"

import os

for root, dirs, files in os.walk(inputDirectory):  
    for filename in files:
        if fname.endswith(inputFileExtension):
            print(filename)

