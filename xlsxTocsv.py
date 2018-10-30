
inputDirectory = "/home/mywh/data/能源局各縣市汽車加油站汽柴油銷售統計表/"
inputFileExtension = "xlsx"

import os

for root, dirs, files in os.walk("."):  
    for filename in files:
        print(filename)

for fname in os.listdir('/home/mywh/data/能源局各縣市汽車加油站汽柴油銷售統計表/107年'):
...   if fname.endswith('.xlsx'):
...     print(fname)
