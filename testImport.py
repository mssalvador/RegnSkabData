'''
Created on Sep 5, 2016

@author: svanhmic
'''
from datetime import date, timedelta
import numpy as np
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.ml.linalg import Vectors,VectorUDT
import pyspark.sql.functions as F
from pyspark.sql import Row

sc = SparkContext("local[8]","testing")
sqlContext = SQLContext(sc)

def sparseA(v1,v2):
    sumArray = v1.toArray()+v2.toArray()
    noneZeros = dict([(i,val) for i,val in enumerate(sumArray) if val > 0.0])
    return Vectors.sparse(v1.size,noneZeros)

def absMult(v1):
    return Vectors.dense(np.abs(v1))

sparseQuickAddU = F.udf(sparseA,VectorUDT())
multUdf = F.udf(lambda x,y: x*y, VectorUDT())
absUdf = F.udf(absMult,VectorUDT())

dataDfDense = sqlContext.createDataFrame([(1.0,Vectors.dense(np.ones([3,1]))),(0.0,Vectors.dense([2.0,4.0,6.1])),(1.0,Vectors.dense([3.0,2.0,1.1]))],["label","vectors"])
dataDfSparse = sqlContext.createDataFrame([(1.0,Vectors.sparse(4,{0:1.0,2:3.0})),(0.0,Vectors.sparse(4,{0:2.0,1:4.0,2:6.1})),(1.0,Vectors.sparse(4,{0:3.0,1:2.0,2:1.1}))],["label","vectors"])

dataRdd = dataDfDense.select("vectors").rdd
dataSparseRdd = dataDfSparse.select("vectors").rdd
#print dataRdd.collect()
newDataDf = dataRdd.cartesian(dataRdd).map(lambda x: Row(v1=x[0]["vectors"],v2=x[1]["vectors"])).toDF(["vector1","vector2"])
#newDataDf.show()
newDataSparseDf = dataSparseRdd.cartesian(dataSparseRdd).map(lambda x: Row(v1=x[0]["vectors"],v2=x[1]["vectors"])).toDF(["vector1","vector2"])
newDataSparseDf.show(truncate = False)

newDataDf.select(multUdf(newDataDf["vector1"],newDataDf["vector2"]).alias("v1*v2"),absUdf(newDataDf["vector1"])).show()
testing = newDataSparseDf.select(sparseQuickAddU(newDataSparseDf["vector1"],newDataSparseDf["vector2"]).alias("spv1*spv2")).show(truncate=False)


if __name__ == '__main__':
    pass