'''
Created on Oct 10, 2016

@author: svanhmic
'''
<<<<<<< HEAD
=======
from pyspark import SparkContext
>>>>>>> 44c1327efbcacd426e3974403ebc8f9a30b76a07
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
import pyspark.sql.types as pyt
from datetime import datetime
import numpy as np
import re
#from RegnskabsClass import Regnskaber

import sys
<<<<<<< HEAD
from pyspark.context import SparkContext
reload(sys)
sys.setdefaultencoding('utf-8')
sc = SparkContext("local[8]","CreateRegnskabsFormat" )#pyFiles=['/home/svanhmic/workspace/Python/Erhvervs/src/RegnSkabData/RegnskabsClass.py'])
sqlContext = SQLContext(sc)
#sc.addPyFile('/home/svanhmic/workspace/Python/Erhvervs/src/RegnSkabData/RegnskabsClass.py')
sc = SparkContext



def testValue(*x):
    '''
    Description: Checks which data type the value is
        
    
    Input: 
        x - 
    
    
    Output:
        output -
    
    '''
=======
reload(sys)
sys.setdefaultencoding('utf-8')

sc = SparkContext("local[8]","CreateRegnskabsFormat" )#pyFiles=['/home/svanhmic/workspace/Python/Erhvervs/src/RegnSkabData/RegnskabsClass.py'])
sqlContext = SQLContext(sc)
#sc.addPyFile('/home/svanhmic/workspace/Python/Erhvervs/src/RegnSkabData/RegnskabsClass.py')

def testValue(*x):
>>>>>>> 44c1327efbcacd426e3974403ebc8f9a30b76a07
    #print(x)
    output = []
    for val in x:
        #reVal = val
        reVal = re.sub(r'\s+', "", val)
        if reVal == "none" or reVal =='':
            output.append(None)
            continue 
        elif re.match(r"(\d\d\d\d-\d.-\d.)",reVal):
            output.append(datetime.strptime(reVal, '%Y-%M-%d'))
            continue
        elif re.match(r'True',reVal):
            output.append(True)
            continue
        elif re.match(r'False',reVal):
            output.append(False)
            continue
        else:
            try:
                removeStopAndReplaceCommas = float(re.sub(",",".",re.sub("\.", "", reVal)))
                if removeStopAndReplaceCommas < 1.0:
                    output.append(removeStopAndReplaceCommas)
                else:
                    output.append(int(removeStopAndReplaceCommas))
                                        
            except ValueError:
                #print("ERROR "+reVal)
                output.append(reVal)
    return output

def getTypes(*x):
    output = []
    for val in x:
        output.append(str(type(val)))
    return output

def getValues(*df):
    return tuple([x[0] for x in df])

def getNumberOfTypes(df):
    
    cols = df.columns
    output = {}
#    nDf = df.drop_duplicates()
    for c in cols:
<<<<<<< HEAD
        Ndf = df.select(c).filter(F.col(c) != "<class 'NoneType'>").distinct().select(F.regexp_replace(F.col(c), r'type|\W', "").alias("types")).cache()
=======
        Ndf = df.select(c).filter(F.col(c) != "<type 'NoneType'>").distinct().select(F.regexp_replace(F.col(c), r'type|\W', "").alias("types")).cache()
>>>>>>> 44c1327efbcacd426e3974403ebc8f9a30b76a07
        count = Ndf.count()
        types = Ndf.collect()
        output[c] = (count,getValues(*types))
        Ndf.unpersist()
    return output

<<<<<<< HEAD
def printList(lis):
    assert isinstance(lis, list), "this is not a list it's a:"+str(type(lis))
    for line in lis:
        print(line)

path = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/csv"
finalXML = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/finalXML"


def main():
    '''
    Description: Main method. 
    '''
    #Extract formattet accounts from csv-files
=======


path = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/csv"
finalXML = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/finalXML"
cleanedCsvPath = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/sparkdata/csv"




#Note to tomorrow Do something GREAT! 
# And divide the plots up such that labels can be seen
if __name__ == '__main__':
>>>>>>> 44c1327efbcacd426e3974403ebc8f9a30b76a07
    dfRegnskabsVal = sqlContext.read.csv(path=cleanedCsvPath+"/pivotRowDataValues.csv", sep=";" , header=True,encoding="utf-8",dateFormat="yyyy-mm-dd",nullValue="none")
    #dfRegnskabsVal.show()
    #dfRegnskabsVal.printSchema()
    dfCols = dfRegnskabsVal.columns
<<<<<<< HEAD
    printList(dfCols)
    #rddOfRows = dfRegnskabsVal.select(dfRegnskabsVal[assetsCols[0]]).rdd.map(lambda x: testValue(*x)).cache() # TESTING PURPOSES ONLY!
    rddOfRows = dfRegnskabsVal.rdd.map(lambda x: testValue(*x)).cache()
    for c in rddOfRows.collect():
        print(c)
    
    OfTypesDf = rddOfRows.map(lambda x: getTypes(*x)).toDF(dfCols).cache()
    OfTypesDf.show()
    
    numberOfTypesPrCol = getNumberOfTypes(OfTypesDf)
    structSchema = pyt.StructType()
#     for (i,cols) in enumerate(dfCols):
#         
#         if numberOfTypesPrCol[cols][0] > 1:
#             if "unicode" in numberOfTypesPrCol[cols][1]:
#                 structSchema.add(cols, pyt.StringType(),True)
#             elif "float" in numberOfTypesPrCol[cols][1]:
#                 structSchema.add(cols, pyt.StringType(),True)
#             print(cols)
#             print(numberOfTypesPrCol[cols])
#         else:
#             colType = numberOfTypesPrCol[cols][1][0]
#             #colType = re.sub(r'\W+',"",colType) 
#             #print(colType)
#             if colType == "unicode":
#                 structSchema.add(cols,pyt.StringType(),True)
#             elif colType == "float":
#                 structSchema.add(cols,pyt.FloatType(),True)
#             elif colType == "int":
#                 structSchema.add(cols,pyt.LongType(),True)
#             elif colType == "datetimedatetime":
#                 structSchema.add(cols,pyt.DateType(),True)
#             else:
#                 structSchema.add(cols,pyt.StringType(),True)
#                 #print(str(cols)+" has been sent down here as a string!")
=======
    assetsCols =["assets"]
    
    #rddOfRows = dfRegnskabsVal.select(dfRegnskabsVal[assetsCols[0]]).rdd.map(lambda x: testValue(*x)).cache() # TESTING PURPOSES ONLY!
    rddOfRows = dfRegnskabsVal.rdd.map(lambda x: testValue(*x)).cache()
    #for c in rddOfRows.collect():
    #    print(c)
    
    OfTypesDf = rddOfRows.map(lambda x: getTypes(*x)).toDF(dfCols).cache()
    #OfTypesDf.show()
    
    numberOfTypesPrCol = getNumberOfTypes(OfTypesDf)
    structSchema = pyt.StructType()
    for (i,cols) in enumerate(dfCols):
        
        if numberOfTypesPrCol[cols][0] > 1:
            if "unicode" in numberOfTypesPrCol[cols][1]:
                structSchema.add(cols, pyt.StringType(),True)
            elif "float" in numberOfTypesPrCol[cols][1]:
                structSchema.add(cols, pyt.StringType(),True)
            print(cols)
            print(numberOfTypesPrCol[cols])
        else:
            colType = numberOfTypesPrCol[cols][1][0]
            #colType = re.sub(r'\W+',"",colType) 
            #print(colType)
            if colType == "unicode":
                structSchema.add(cols,pyt.StringType(),True)
            elif colType == "float":
                structSchema.add(cols,pyt.FloatType(),True)
            elif colType == "int":
                structSchema.add(cols,pyt.LongType(),True)
            elif colType == "datetimedatetime":
                structSchema.add(cols,pyt.DateType(),True)
            else:
                structSchema.add(cols,pyt.StringType(),True)
                #print(str(cols)+" has been sent down here as a string!")
>>>>>>> 44c1327efbcacd426e3974403ebc8f9a30b76a07
        #print("col "+str(i)+" val "+str(v))
    
    #print(structSchema)
    
<<<<<<< HEAD
    #dataWithSchema = sqlContext.createDataFrame(rddOfRows,structSchema)
    #dataWithSchema.distinct().show()
    #dataWithSchema.write.csv(cleanedCsvPath+"/formattetreskabsdata.csv",sep=";", header=True,mode="overwrite")


#Note to tomorrow Do something GREAT! 
# And divide the plots up such that labels can be seen
if __name__ == '__main__':
    main()
=======
    dataWithSchema = sqlContext.createDataFrame(rddOfRows,structSchema)
    #dataWithSchema.distinct().show()
    dataWithSchema.write.csv(cleanedCsvPath+"/formattetreskabsdata.csv",sep=";", header=True,mode="overwrite")
>>>>>>> 44c1327efbcacd426e3974403ebc8f9a30b76a07
    
    
    
    