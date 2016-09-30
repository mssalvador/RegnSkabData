'''
Created on Sep 26, 2016

@author: svanhmic
'''
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vector,VectorUDT
import pyspark.sql.functions as F
import os
import re
from datetime import datetime
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

toDenseVectorUDF = F.udf(lambda x: x.toArray(), VectorUDT())

df = sqlContext.read.csv(cleanedCsvPath+"/regnskabsdata.csv", sep=";", header=True,encoding="utf-8") 
#df.show()

removeBlanksDf = (df
                  .filter(df["id"] != np.NaN)
                  .select(df["name"],df["id"],F.regexp_replace(df["value"], r'(\s+) ',"").alias("value"),df["startDate"],df["endDate"]))
#removeBlanksDf.show(truncate=False)

pivotCount = removeBlanksDf.groupby("id").pivot("name").count().fillna(0)
#pivotCount.show()
pivotCols = pivotCount.columns

vecAssempler = VectorAssembler(inputCols=pivotCols[1:],outputCol="features")
vectorizedDf = vecAssempler.transform(pivotCount).select(F.col("id"),F.col("features").alias("features"))
countVector = vectorizedDf.collect()
idVec = [v["id"] for v in countVector]
featureVec = np.array([v["features"].toArray() for v in countVector])
#print()
lineOfSet = np.linspace(0,2*(featureVec.shape[0]+1), num=featureVec.shape[0])
linelengths = np.ones(featureVec.shape[0])
plt.eventplot(featureVec,lineoffsets=lineOfSet, linelengths=linelengths)
#plt.yscale("log")
plt.show()   
#pivotedDataDf = filteredValuesDf.groupby(filteredValuesDf["id"],filteredValuesDf["startDate"],filteredValuesDf["endDate"]).pivot("name").agg(F.sum("value"))


if __name__ == '__main__':
    pass