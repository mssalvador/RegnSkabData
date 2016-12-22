'''
Created on Sep 26, 2016

@author: svanhmic
'''
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
import pyspark.sql.functions as F
import pyspark.sql.types as pyt
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors,VectorUDT
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
import re
#from RegnskabsClass import Regnskaber

sc = SparkContext("local[8]","TestPyspark")#pyFiles=['/home/svanhmic/workspace/Python/Erhvervs/src/RegnSkabData/RegnskabsClass.py'])
sqlContext = SQLContext(sc)
#sc.addPyFile('/home/svanhmic/workspace/Python/Erhvervs/src/RegnSkabData/RegnskabsClass.py')
path = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/csv"
finalXML = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/finalXML"
cleanedCsvPath = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/sparkdata/csv"

#Note to tomorrow Do something GREAT! 
# And divide the plots up such that labels can be seen
if __name__ == '__main__':
    dfRegnskabsVal = sqlContext.read.csv(path=cleanedCsvPath+"/pivotRowDataValues.csv", sep=";" , header=True,encoding="utf-8",dateFormat="yyyy-mm-dd",nullValue="none")
    #df = sqlContext.read.csv(cleanedCsvPath+"/formattetreskabsdata.csv",sep=";", header=True, encoding="utf-8",inferSchema=True,dateFormat="yyyy-mm-dd",)
    dfRegnskabsVal.printSchema()
    for line in dfRegnskabsVal.take(10):
        print(line)
    #print(df.select(df["DisclosureOfInvestments"]).orderBy(F.col("DisclosureOfInvestments").desc()).distinct().take(10))
    #dfColNameAndTypes = df.d    types
    #stringColsInDf = map(lambda x: str(x[0]),filter(lambda x: x[1] != "string",dfColNameAndTypes))
    #df.select(stringColsInDf[470:480]).show()
    #print(len(stringColsInDf))
    #print()
    #for p in stringColsInDf:
    #    print(p)
    #print(len(stringColsInDf))
    #for assets in df.select(F.regexp_extract(df["assets"],r'(\D+)',0).alias("assets")).sort(F.col("assets").desc()).distinct().collect():
    #    print(assets)
    
    
        
    
    