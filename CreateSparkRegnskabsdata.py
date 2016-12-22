'''
Created on Oct 5, 2016

@author: svanhmic
@license: Apache 2.0
@summary: Imports a compiled csv file with accounts and transforms them into a new csv file containing all unique columns from all accounts. 
Meaning one row will consist of an accoount.
'''
from pyspark import SparkContext
from pyspark.sql import SQLContext,SparkSession
from pyspark.sql.types import StructType,StringType,StructField
import pyspark.sql.functions as F
import numpy as np
import sys

def createDfStruct(inputList):
    '''
        Transforms a list of columns to a struct to be parsed into a spark dataframe
        
        Input:
            -inputList: Input list with columns
        Output:
            -outputStruct: StructType() element to be used in a dataframe
    '''
    
    return StructType([StructField(str(n),StringType(),nullable=True) for n in inputList])
  


def PivotForAllCols(sqlContext,df):
    '''
    @summary: Refactoring. Takes all uniqe columns and creates a giant pivot table
    
    Input: 
        df - Spark Data Frame that contains all accounts.
    Output:  
        transformedDf - Spark Data Frame that includes all unique columns meaning one row contains an entire account.
    '''
    removeBlanksDf = (df
                      .filter(df["id"] != np.NaN)
                      .select(df["name"],df["id"],F.regexp_replace(df["value"], r'(\s+) ',"").alias("value"),df["unit"],df["startDate"],df["endDate"]))
    #removeBlanksDf.show(truncate=False)
    #removeBlanksDf.select(removeBlanksDf["name"]).distinct().show(truncate=False)
    #print("Number of unique columns: "+str(removeBlanksDf.select(removeBlanksDf["name"]).distinct().count()))
     
    #Does the pivotation without respect to dimension
    specialPivotDf = (removeBlanksDf
                      .dropDuplicates()
                      .groupby("id","name","value","startDate","endDate","unit")
                      .count()
                      .groupby("id")
                      .pivot("name")
                      .agg(F.max(F.struct("count","value"))))
    
    #print(specialPivotDf.take(1))
    #specialPivotDf.printSchema()
    specialPivotColumns = specialPivotDf.columns

    pivotValues = (specialPivotDf
                   .rdd
                   .map(lambda x: [None if x[specialPivotColumns[i]] is None else x[specialPivotColumns[i]] if x[specialPivotColumns[i]][1] is None else x[specialPivotColumns[i]][1] for i in range(len(specialPivotColumns))])) # create an rdd with data values for a 
    output =  sqlContext.createDataFrame(pivotValues,createDfStruct(specialPivotColumns))
    output.printSchema()
    output.write.csv(cleanedCsvPath+"/pivotRowDataValues.csv", sep=";", header=True,mode="overwrite")
    return output    
    
def main(args):
    #Read data 
    sc = SparkContext(master="local[8]",appName="CreateRegnskabsData")#pyFiles=['/home/svanhmic/workspace/Python/Erhvervs/src/RegnSkabData/RegnskabsClass.py'])
    spark = SparkSession(sc)
    sqlContext = SQLContext(sc,sparkSession=spark)
    df = sqlContext.read.csv(cleanedCsvPath+"/regnskabsdata.csv", sep=";", header=True,encoding="utf-8") 
    #df.show()
    transformedDf = PivotForAllCols(sqlContext,df)
    #transformedDf.write.csv(cleanedCsvPath+"/pivotRowDataValues.csv", sep=";", header=True,mode="overwrite")
    
if __name__ == '__main__':
    
    if len(sys.argv) == 0:
        cleanedCsvPath = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/sparkdata/csv"
        cleanedParquePath = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/sparkdata/parquet"
    else:
        cleanedCsvPath = sys.argv[1]
    #print(cleanedCsvPath)
    main(sys.argv)
    
    
    
    
    