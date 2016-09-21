'''
Created on Jun 13, 2016

@author: svanhmic
'''

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType
import pyspark.sql.functions as F
import os
import re
# HACK!!!
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

sc = SparkContext("local[8]","importRegnskabs")
sqlContext = SQLContext(sc)
folderPath = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/testcsv"

stringRowConcat = F.udf(lambda x,y,z,v: x+";"+y+";"+z+";"+v, StringType())
removeInFrontUdf = F.udf(lambda col: re.sub(r'\w+:', "", col),StringType())

files = os.listdir(folderPath)
print files
df = sqlContext.read.csv(folderPath+"/",header=True) # Read the entire folder of csv's, pretty neat
columns = df.columns
df = (df.withColumnRenamed(columns[0],"Name"))
print df.columns
print df.count()

dfNames = df.filter(df.Value != None) # Removes all those how have no value, it's generally noise.
dfNames.show(truncate=False)

distinctDfNames = (dfNames
                   .select(removeInFrontUdf(dfNames["Name"]).alias("Name"),dfNames["EntityIdentifier"])
                   .distinct())
groupDistinctNamesDf = distinctDfNames.groupby(distinctDfNames["Name"]).count()
#joinedWithCvr = distinctDfNames.join(groupDistinctNamesDf,distinctDfNames["Name"] == groupDistinctNamesDf["Name"],'inner').drop(groupDistinctNamesDf["Name"])
groupDistinctNamesDf.show()
for n in sorted(groupDistinctNamesDf.collect()): 
    print n["Name"] , " , " , n['count']


#dfOneCol = dfNames.select(stringRowConcat(dfNames["Name"],dfNames["Value"],dfNames["Start"],dfNames["End/Instant"]))
#dfOneCol.show(truncate=False)
#print dfOneCol.count() 


