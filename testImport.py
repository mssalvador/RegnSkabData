'''
Created on Sep 5, 2016

@author: svanhmic
'''
from datetime import date, datetime
import numpy as np
from scipy.sparse import csc_matrix
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.ml.linalg import Vectors,VectorUDT
import pyspark.sql.functions as F
from pyspark.sql import Row

sc = SparkContext("local[8]","testing")
sqlContext = SQLContext(sc)

def absMult(v1):
    return Vectors.dense(np.abs(v1))

multUdf = F.udf(lambda x,y: x*y, VectorUDT())
absUdf = F.udf(absMult,VectorUDT())

dataDfDense = sqlContext.createDataFrame([(1.0,Vectors.dense(np.ones([3,1]))),(0.0,Vectors.dense([2.0,4.0,6.1])),(1.0,Vectors.dense([3.0,2.0,1.1]))],["label","vectors"])
dataDfSparse = sqlContext.createDataFrame([(1.0,Vectors.sparse(3,{0:1.0,1:2.0,2:3.0})),(0.0,Vectors.sparse(3,{0:2.0,1:4.0,2:6.1})),(1.0,Vectors.sparse(3,{0:3.0,1:2.0,2:1.1}))],["label","vectors"])

dataRdd = dataDfDense.select("vectors").rdd
dataSparseRdd = dataDfSparse.select("vectors").rdd
#print dataRdd.collect()
newDataDf = dataRdd.cartesian(dataRdd).map(lambda x: Row(v1=x[0]["vectors"],v2=x[1]["vectors"])).toDF(["vector1","vector2"])
newDataDf.show()
newDataSparseDf = dataSparseRdd.cartesian(dataSparseRdd).map(lambda x: Row(v1=x[0]["vectors"],v2=x[1]["vectors"])).toDF(["vector1","vector2"])
newDataSparseDf.show()

newDataDf.select(multUdf(newDataDf["vector1"],newDataDf["vector2"]).alias("v1*v2"),absUdf(newDataDf["vector1"])).show()

row = np.array([0,2])
col = np.array([0,0])
sv2 = csc_matrix((np.array([1.0, 3.0]),(row,col)), shape = (3, 1))
print sv2

if __name__ == '__main__':
    pass