'''
Created on Jun 13, 2016

@author: svanhmic
'''

from pyspark import SparkContext
from pyspark.sql import SQLContext


sc = SparkContext("local[4]","pdfapp")
sqlContext = SQLContext(sc)
folderPath = "/home/svanhmic/workspace/Python/ErhvervStyrrelsenDown/data/regnskabsdata/xml/"

df = sqlContext.read.format('com.databricks.spark.xml').options(rowTag='xbrl').load(folderPath+"cvr10783445.xml")
newDf = df.count()
print df.columns
#print 
#print df.select("g:DescriptionOfMethodsOfRecognitionAndMeasurementBasisOfReceivables",'g:OtherInterestIncome').show()

