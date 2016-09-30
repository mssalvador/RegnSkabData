'''
Created on Jun 13, 2016

@author: svanhmic
'''

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.types import StringType,StructType, ArrayType,IntegerType,DateType
import pyspark.sql.functions as F
import os
import re
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
from RegnskabsClass import Regnskaber
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

sc = SparkContext("local[8]","importRegnskabs")
sqlContext = SQLContext(sc)
sc.addPyFile('/home/svanhmic/workspace/Python/Erhvervs/src/RegnSkabData/RegnskabsClass.py') # this adds the class regnskabsClass to the spark execution
folderPath = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/csv"
finalXML = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/finalXML"

def convertToDate(col):
    try:
        return datetime.strptime(col,'%Y-%M-%d')
    except:
        return None


def extractFilesForTaxonomy(fileNamesDf,taxTypeDf):
    minDf = taxTypeDf.select(F.concat(F.regexp_extract("file",'(\w+)/(\d+-\d+-\d+.xml)',2),F.lit(".csv")).alias("file"),taxTypeDf["taxonomy"]).cache()
    #minDf.show(5,truncate=False)
    
    intersectFilesDf = (fileNamesDf
                        .join(minDf,minDf["file"] == fileNamesDf["file"],"inner")
                        .drop(fileNamesDf["file"])) # join list fo files with list of files with taxonomy, so we can single out those records we want to analyze
    #intersectFilesDf.show(20,truncate=False)

    groupedIntersectFilesDf = intersectFilesDf.groupBy("taxonomy").count()
    #groupedIntersectFilesDf.orderBy(groupedIntersectFilesDf["count"].desc()).show(truncate=False) # show the different types of tax'
    
    mostTaxonomy = groupedIntersectFilesDf.orderBy(groupedIntersectFilesDf["count"].desc()).first()["taxonomy"]
    print(mostTaxonomy)
    filteredCsvDf = intersectFilesDf.filter(intersectFilesDf["taxonomy"] == mostTaxonomy)
    return [str(f["file"]) for f in filteredCsvDf.collect()]

#regnskab = Regnskaber(files[0])
lengthUdf = F.udf(lambda x: len(x), IntegerType())
convertToDateUdf = F.udf(convertToDate,DateType())

files = os.listdir(folderPath)
fileNamesDf = sqlContext.createDataFrame([Row(file=f) for f in files]) # import of csv files to dataframe   

struct = StructType().add("file",StringType(),True).add("taxonomy",StringType(),True)
taxTypeDf = sqlContext.read.csv(finalXML+"/taxlist.csv",header=False,schema=struct,sep=";")

recordList = extractFilesForTaxonomy(fileNamesDf,taxTypeDf)
#print(recordList)
list = []
for f in recordList:
    list.append(Regnskaber(folderPath+"/"+f))
    
print("Done with all file")

df = sqlContext.createDataFrame(list)
del(list)
df.printSchema()
df.show()
#print(df.take(1))
listCsvDf = df.drop("file").select(F.explode(F.col("field")))
valueCsvDf = listCsvDf.select(F.regexp_replace(listCsvDf["col"]["name"],r"\w+:","").alias("name")
                               ,listCsvDf["col"]["id"].cast("integer").alias("id")
                               ,listCsvDf["col"]["value"].alias("value")
                               ,listCsvDf["col"]["startDate"].alias("startDate")
                               ,listCsvDf["col"]["endDate"].alias("endDate"))

valueCsvDf.show(truncate=False)
#orderedListCsvDf = listCsvDf.orderBy(listCsvDf["fieldlength"].desc()).select(listCsvDf["fieldlength"])
#newDf = df.select(df["field"]["name"].alias("name"),df["field"]["value"].alias("value"))
valueCsvDf.write.csv("/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/sparkdata/csv/regnskabsdata.csv",mode='overwrite',header=True,sep=";")



#plt.eventplot(variableOccurance)
#plt.show()
#print(variableOccurance[0])

