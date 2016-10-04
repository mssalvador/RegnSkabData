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

def doSomething(df):
    
    for c in df.columns:
        df.withColumnRenamed("new","old")    
    return None

def wordOccurence(colFeature):
    col = colFeature.toArray()
        
    return col

path = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/csv"
finalXML = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/finalXML"
cleanedCsvPath = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/sparkdata/csv"

toDenseVectorUDF = F.udf(lambda x: x.toArray(), VectorUDT())
getIndicesUdf = F.udf(lambda x: Vectors.dense(x.indices),VectorUDT())

df = sqlContext.read.csv(cleanedCsvPath+"/regnskabsdata.csv", sep=";", header=True,encoding="utf-8") 
#df.show()

removeBlanksDf = (df
                  .filter(df["id"] != np.NaN)
                  .select(df["name"],df["id"],F.regexp_replace(df["value"], r'(\s+) ',"").alias("value"),df["startDate"],df["endDate"]))
#removeBlanksDf.show(truncate=False)
removeBlanksDf.select(removeBlanksDf["name"]).distinct().show(truncate=False)
print(removeBlanksDf.select(removeBlanksDf["name"]).distinct().count())
pivotCount = removeBlanksDf.groupby("id").pivot("name").count().fillna(0)

specialPivotDf = removeBlanksDf.dropDuplicates().groupby("id","name","value","startDate","endDate").count().groupby("id").pivot("name").agg(F.max(F.struct("count","value")))
#specialPivotDf.show(1)
#specialPivotDf.printSchema()
specialPivotColumns = specialPivotDf.columns
pivotStructVals = StructType()
pivotStructCount = StructType()
for c in specialPivotColumns:
    pivotStructVals.add(c,StringType(), True)
    pivotStructCount.add(c, IntegerType(), True)

#print(specialPivotColumns)
#pivotValues = (specialPivotDf
#               .rdd
#               .map(lambda x: [None if x[specialPivotColumns[i]] is None else x[specialPivotColumns[i]] if x[specialPivotColumns[i]][1] is None else x[specialPivotColumns[i]][1] for i in range(len(specialPivotColumns))]))
#transformedDf = sqlContext.createDataFrame(pivotValues,pivotStructVals)

#print(transformedDf.printSchema())
#transformedDf.show()

pivotCountRdd = (specialPivotDf
                .rdd
                .map(lambda x: [0 if x[specialPivotColumns[i]] is None else 1 for i in range(len(specialPivotColumns))]))
pivotCountDf = sqlContext.createDataFrame(pivotCountRdd,pivotStructCount)
#pivotCountDf.show()

groupedPivotCount = (pivotCountDf
                       .groupBy()
                       .avg()
                       .collect())

countDict = groupedPivotCount[0].asDict()
for (k,v) in countDict.items():
    print("keys: " + str(k)+" Values: "+str(v))

fig = plt.figure(1)
ax = fig.add_subplot(111)
ind = np.arange(len(specialPivotColumns))
widt = 0.35
plot1 = ax.bar(ind,countDict.values(),widt,color="green")
ax.set_ylim(0,1.1)
ax.set_xticks(ind+widt)
xtickNames = ax.set_xticklabels(countDict.keys())
plt.setp(xtickNames, rotation=45, fontsize=10)
plt.show()
    
#Note to tomorrow Do something GREAT! 
# And divide the plots up such that labels can be seen
if __name__ == '__main__':
    pass