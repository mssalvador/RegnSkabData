'''
Created on Jun 13, 2016

@author: svanhmic

This script creates a compiled csv file with the test csv records. 
The csv files containing the accounts are converted from a column-based representation to a row based

representation. meaning:
account alpha
var x , val x date x
var y , val y date y
...

to 

account aplha [var x, val x , date x], beta[var x , val x , date x] 

'''



from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import StringType,StructType,StructField,StructType,IntegerType,DateType,ArrayType
import pyspark.sql.functions as F

sc = SparkContext("local[*]","importRegnskabs")
sqlContext = SQLContext(sc)
#sc.addPyFile('/home/svanhmic/workspace/Python/Erhvervs/src/RegnSkabData/RegnskabsClass.py') # this adds the class regnskabsClass to the spark execution



import os
import re
from datetime import datetime
from RegnskabsClass import Regnskaber
import sys


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

def toDataFrame(file):
    '''
        Creates a dataframe from a csv file
    '''
    
    df = sqlContext.read.csv(path=file, sep=";", encoding="utf-8",ignoreTrailingWhiteSpace=True, nullValue="", nanValue="")
    return df

def getAllTaxFiles(path):
    
    return list(filter(lambda x: "taxlist.csv" in x,os.listdir(path)))

def encodes(x):
    try:
        return x.encode('ascii', 'replace')
    except:
        return None 

def main():
    
    argLen = len(sys.argv)
    csvLocation = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/cleanCSV"
    outputLocation = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/sparkdata/parquet"
    if argLen == 2:
        csvLocation = sys.argv[1]
    elif argLen == 3:
        csvLocation = sys.argv[1]
        outputLocation = sys.argv[2]
    elif argLen == 3:
        csvLocation = sys.argv[1]
        outputLocation = sys.argv[2]

    taxFiles = getAllTaxFiles(csvLocation)
    taxList = [csvLocation+"/"+i for i in taxFiles]

    
    xmlTaxSchema = StructType().add("path",StringType(), True).add("taxonomy", StringType(), True)
    taxListDf = (sqlContext
                 .read
                 .format('com.databricks.spark.csv')
                 .options(sep=";", encoding="utf-8")
                 .load(taxList,schema=xmlTaxSchema))
    
    csvlist = [csvLocation+"/"+f for f in os.listdir(csvLocation) if f not in taxFiles] 
      
    regnskabRowSchema = (StructType()
                         .add(field="Name", data_type=StringType(), nullable=True)
                         .add(field="Dec", data_type=StringType(), nullable=True)
                         .add(field="Prec", data_type=StringType(), nullable=True)
                         .add(field="Lang", data_type=StringType(), nullable=True)
                         .add(field="unitRef", data_type=StringType(), nullable=True)
                         .add(field="contextRef", data_type=StringType(), nullable=True)
                         .add(field="EntityIdentifier", data_type=StringType(), nullable=True)
                         .add(field="Start", data_type=StringType(), nullable=True)
                         .add(field="End_Instant", data_type=StringType(), nullable=True)
                         .add(field="Value", data_type=StringType(), nullable=True)
                         .add(field="Dimensions", data_type=StringType(), nullable=True))
    
    
    df = (sqlContext
          .read
          .format('com.databricks.spark.csv')
          .options(sep=",",encoding='utf8',header=True,nullValue=None,nanValue=None)
          .load(csvlist,schema=regnskabRowSchema))

    cols = df.columns
    uniCodeUdf = F.udf(lambda x: encodes(x), StringType())
    stringCols = [uniCodeUdf(F.col(i[0])).alias(i[0]) if i[1] == "string" else i[0] for i in df.dtypes]   
    
    alteredDf = df.select(stringCols)
    alteredDf.show(20)
    
    #write the dataframe to parquet file
    (alteredDf
     .write
     .parquet(outputLocation+"/regnskaber.parquet",mode="overwrite"))
    
if __name__ == '__main__':
    main()

