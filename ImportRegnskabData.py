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
sc.addPyFile('/home/svanhmic/workspace/Python/Erhvervs/src/RegnSkabData/RegnskabsClass.py') # this adds the class regnskabsClass to the spark execution



import os
import re
from datetime import datetime
from RegnskabsClass import Regnskaber
import sys
import csv

def convertToDate(col):
    try:
        return datetime.strptime(col,'%Y-%M-%d')
    except:
        return None

def removeNewlineChars(file):
    
    fieldNames = []
    newRows = []
    with open(file,"r",) as csvfile:
        file = csv.reader(csvfile,delimiter="|",dialect='excel')
        #print(type(file))
        for r in file:
            #print(r)
            newR = [(i.replace("\n",""))for i in r]
            #print(newR)
            newRows.append(newR)
    return newRows
    
def writeToFile(ars,file):
    with open(file,"w+") as outputcsv:
        outputFile = csv.writer(outputcsv,delimiter="|",quoting=csv.QUOTE_ALL,dialect='excel',quotechar='"')
        outputFile.writerows(ars)

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
        return x.encode("ascii","xmlcharrefreplace")
    #.encode("ascii", "replace")
    except:
        return None

    
def stringToArray(col):
    '''
        changes the string column to an array column
    '''
    removeSigns = re.compile(r'[^\w\.,:]',flags=re.IGNORECASE)
    try: 
        words = removeSigns.sub('',col).split(",")
    except:
        words = []
    return words



def moveFiles(source,dest):
    
    files = os.listdir(source)
    try:
        [os.rename(src=source+"/"+i,dst=dest+"/"+i) for i in files if "taxlist" in i]
        return "done"
    except:
        return None

def lend(x):
    try:
        return len(x)
    except:
        return 0

lenUdf = F.udf(lambda x: lend(x),IntegerType())

def showColums(df,funcs,*args):
    
    aggedCols = [f(i) for i in args for f in funcs]
    TypeSelectDf = (df.groupBy(*args)
                    .agg(*aggedCols)
                   )
                    
    TypeSelectDf.show(len(TypeSelectDf.collect()))
    
def extr(x):
    extracted = re.sub(pattern=r'<.*?>|\t',repl="",string=str(x),flags=re.MULTILINE|re.IGNORECASE|re.VERBOSE)
    try:
        return extracted
    except AttributeError as ae:
        print("wrong!!!")
        return None


def convertToSym(x):
    matched = dict(zip(re.findall(string=x,pattern=r'(&#\d+;)'),[chr(int(i)) for i in re.findall(string=x,pattern=r'(?<=&#)\d+')]))
    #print(matched)
    #print(replaceAll(x,matched))
    return replaceAll(x,matched)
    
    
def replaceAll(text,dic):
    for i in dic.items():
        text = text.replace(i[0], i[1])
    return text

   

def main():
    
    argLen = len(sys.argv)
    csvLocation = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/cleanCSV"
    outputLocation = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/sparkdata/parquet"
    taxLocation = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/cleanTaxLists"
    
    if argLen == 2:
        csvLocation = sys.argv[1]
    elif argLen == 3:
        csvLocation = sys.argv[1]
        outputLocation = sys.argv[2]
    elif argLen == 4:
        csvLocation = sys.argv[1]
        outputLocation = sys.argv[2]
        taxLocation = sys.argv[3]
    
    allFiles = os.listdir(csvLocation)
    
    #initial preprocessing
    for f in allFiles:
        writeToFile(removeNewlineChars(csvLocation+"/"+f),csvLocation+"/"+f)
        
    #move taxlists to anotherfolder
    moveFiles(csvLocation,taxLocation)
    
    
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
          .options(sep="|",encoding='utf8',header=True,nullValue="\n",dialect='excel',qoutes='"',lineterminator='^M',parserLib="UNIVOCITY")
          .load(csvLocation+"/*.csv",schema=regnskabRowSchema))
    
    #print(df.filter((F.col("Name")=="arr:StatementOfAuditorsResponsibilityForAuditAndAuditPerformed")&(F.col("EntityIdentifier")=="29241422")).collect())
    #cols = df.columns
    uniCodeUdf = F.udf(lambda x: encodes(x), StringType())
    exUdf = F.udf(lambda x: extr(x),StringType())
    lenUdf = F.udf(lambda x: lend(x),IntegerType())
    stringToArrayUdf = F.udf(lambda x: stringToArray(x),ArrayType(StringType(),True))
    
    
    stringCols = [uniCodeUdf(F.col(i[0])).alias(i[0]) if i[1] == "string" else i[0] for i in df.dtypes]   
    
    alteredDf = (df
                 .select(stringCols)
                 .withColumn(col=F.regexp_replace(F.col("EntityIdentifier")," ","").cast("integer"),colName="EntityIdentifier")
                 .withColumn(col=stringToArrayUdf(F.col("Dimensions")),colName="Dimensions")
                 .withColumn(col=F.col("Start").cast("date"),colName="Start")
                 .withColumn(col=F.col("End_Instant").cast("date"),colName="End_Instant")
                 .withColumn(col=F.regexp_replace(F.col("unitRef"),r'\w+:',""),colName="unitRef")
                 .withColumn(col=exUdf(F.col("Value")),colName="Value")
                 .withColumn(col=lenUdf(F.col("Value")),colName="originalLength")
                )
    alteredDf.show(50)
    alteredDf.printSchema()
    
    #write the dataframe to parquet file
    (alteredDf
     .write
     .parquet(outputLocation+"/regnskaber.parquet",mode="overwrite"))
    
if __name__ == '__main__':
    main()

