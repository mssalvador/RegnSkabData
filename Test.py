'''
Created on Sep 14, 2016

@author: svanhmic
'''

from RegnskabsClass import Regnskaber
import os

path = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/csv"

files = os.listdir(path)
#regnskab = Regnskaber(files[0])
lsit = []
for f in files:
    lsit.append(Regnskaber(path+"/"+f))
    
print("Done with all file")
print(lsit[0].field)

with open(path+"/taxlist.csv","r") as csvFile:
    doc = csvFile.read()
if __name__ == "__main__":
    pass
    