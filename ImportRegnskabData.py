'''
Created on Jun 13, 2016

@author: svanhmic

<<<<<<< HEAD
This script creates a compiled csv file with the test csv records. 
The csv files containing the accounts are converted from a column-based representation to a row based
=======
This script creates a compiled csv file with the test csv records. The csv files containing the accounts are converted from a column-based representation to a row based
>>>>>>> 44c1327efbcacd426e3974403ebc8f9a30b76a07
representation. meaning:
account alpha
var x , val x date x
var y , val y date y
...

to 

account aplha [var x, val x , date x], beta[var x , val x , date x] 

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
<<<<<<< HEAD
#reload(sys)
#sys.setdefaultencoding('utf-8')
=======
reload(sys)
sys.setdefaultencoding('utf-8')
>>>>>>> 44c1327efbcacd426e3974403ebc8f9a30b76a07

sc = SparkContext("local[8]","importRegnskabs")
sqlContext = SQLContext(sc)
sc.addPyFile('/home/svanhmic/workspace/Python/Erhvervs/src/RegnSkabData/RegnskabsClass.py') # this adds the class regnskabsClass to the spark execution
<<<<<<< HEAD
folderPath = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/testcsv"
finalXML = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/finalXML"
sparkDataLoc = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/sparkdata/csv"
=======
folderPath = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/csv"
finalXML = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/finalXML"
>>>>>>> 44c1327efbcacd426e3974403ebc8f9a30b76a07

def convertToDate(col):
    try:
        return datetime.strptime(col,'%Y-%M-%d')
    except:
        return None


def extractFilesForTaxonomy(fileNamesDf,taxTypeDf):
    '''
    Method: This method compares the csv-files in a folder and checks whether the csv-file has a taxonomy.
    
    Input:
        fileNamesDf - Spark Data frame that contains the files that
        taxTypeDf -  Spark Data frame that contains the taxonomys for all csv-files
    
    Output:
        A list of csv-files that contains the taxonomy with most occurences 
    
    '''
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

<<<<<<< HEAD
def main():
=======
#regnskab = Regnskaber(files[0])
if __name__ == '__main__':
>>>>>>> 44c1327efbcacd426e3974403ebc8f9a30b76a07
    lengthUdf = F.udf(lambda x: len(x), IntegerType()) # user def methods 
    convertToDateUdf = F.udf(convertToDate,DateType()) # user def methods
    
    files = os.listdir(folderPath) # gets all the files in csv
<<<<<<< HEAD
    print(len(files))
=======
>>>>>>> 44c1327efbcacd426e3974403ebc8f9a30b76a07
    fileNamesDf = sqlContext.createDataFrame([Row(file=f) for f in files]) # import of csv files to dataframe   
    
    struct = StructType().add("file",StringType(),True).add("taxonomy",StringType(),True)
    taxTypeDf = sqlContext.read.csv(finalXML+"/taxlist.csv",header=False,schema=struct,sep=";")
    
<<<<<<< HEAD
    #take out only the accounts with highest occurrence of taxonomy 
    recordList = extractFilesForTaxonomy(fileNamesDf,taxTypeDf)
    print("recordlist length: "+str(len(recordList)))
    #list = []
    #for f in recordList:
    #    list.append(Regnskaber(folderPath+"/"+f))
        
    print("Done with all file")
    
    df = sqlContext.createDataFrame([Regnskaber(folderPath+"/"+f) for f in recordList])
    #del(list)
    df.printSchema()
    df.show()
    print(df.count())
=======
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
>>>>>>> 44c1327efbcacd426e3974403ebc8f9a30b76a07
    #print(df.take(1))
    listCsvDf = df.drop("file").select(F.explode(F.col("field")))
    valueCsvDf = listCsvDf.select(F.regexp_replace(listCsvDf["col"]["name"],r"\w+:","").alias("name")
                                   ,listCsvDf["col"]["id"].cast("integer").alias("id")
                                   ,listCsvDf["col"]["value"].alias("value")
                                   ,listCsvDf["col"]["unit"].alias("unit")
                                   ,listCsvDf["col"]["contextRef"].alias("contextRef")
                                   ,listCsvDf["col"]["startDate"].alias("startDate")
                                   ,listCsvDf["col"]["endDate"].alias("endDate"))
    
<<<<<<< HEAD
    #valueCsvDf.show(truncate=False)
    #orderedListCsvDf = listCsvDf.orderBy(listCsvDf["fieldlength"].desc()).select(listCsvDf["fieldlength"])
    #newDf = df.select(df["field"]["name"].alias("name"),df["field"]["value"].alias("value"))
    valueCsvDf.write.csv(sparkDataLoc+"/regnskabsdata.csv",mode='overwrite',header=True,sep=";")
#regnskab = Regnskaber(files[0])
if __name__ == '__main__':
    main()
=======
    valueCsvDf.show(truncate=False)
    #orderedListCsvDf = listCsvDf.orderBy(listCsvDf["fieldlength"].desc()).select(listCsvDf["fieldlength"])
    #newDf = df.select(df["field"]["name"].alias("name"),df["field"]["value"].alias("value"))
    valueCsvDf.write.csv("/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/sparkdata/csv/regnskabsdata.csv",mode='overwrite',header=True,sep=";")
    
    
    
    #plt.eventplot(variableOccurance)
    #plt.show()
    #print(variableOccurance[0])
>>>>>>> 44c1327efbcacd426e3974403ebc8f9a30b76a07

