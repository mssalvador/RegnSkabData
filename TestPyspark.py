'''
Created on Sep 26, 2016

@author: svanhmic
'''
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.types import IntegerType,StructType,StringType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors,VectorUDT
import pyspark.sql.functions as F
import os
import re
import numpy as np
import matplotlib.pyplot as plt
#from RegnskabsClass import Regnskaber

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

sc = SparkContext("local[8]","TestPyspark" )#pyFiles=['/home/svanhmic/workspace/Python/Erhvervs/src/RegnSkabData/RegnskabsClass.py'])
sqlContext = SQLContext(sc)
#sc.addPyFile('/home/svanhmic/workspace/Python/Erhvervs/src/RegnSkabData/RegnskabsClass.py')

path = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/csv"
finalXML = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/finalXML"
cleanedCsvPath = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/sparkdata/csv"

dfRegnskabsCount = sqlContext.read.csv(path=cleanedCsvPath+"/pivotRowDataCounts.csv", sep=";", header=True, encoding="utf-8",inferSchema=True)
#dfRegnskabsCount.show()

groupedPivotCount = (dfRegnskabsCount
                       .groupBy()
                       .avg()
                       .collect())
print(groupedPivotCount)
countDict = groupedPivotCount[0].asDict()
countDict08 = {}
countDict06 = {}
countDict04 = {}
countDict02 = {}
countDict00 = {}

for (k,v) in countDict.items():
    if v >= 0.8:
        countDict08[k] = v
    elif v >= 0.6 and v < 0.8:
        countDict06[k] = v
    elif v >= 0.4 and v < 0.6:
        countDict04[k] = v
    elif v >= 0.2 and v < 0.4:
        countDict02[k] = v
    else:
        countDict00[k] = v
    print("keys: " + str(k)+" Values: "+str(v))

fig = plt.figure(1)
ax = fig.add_subplot(111)
ind = np.arange(len(countDict08))
widt = 0.35
plot1 = ax.bar(ind,countDict08.values(),widt,color="green")
ax.set_ylim(0,1.1)
ax.set_xticks(ind)
xtickNames = ax.set_xticklabels(countDict08.keys())
plt.setp(xtickNames, rotation=45, fontsize=10)
plt.show()
    
#Note to tomorrow Do something GREAT! 
# And divide the plots up such that labels can be seen
if __name__ == '__main__':
    pass