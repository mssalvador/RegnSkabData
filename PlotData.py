'''
Created on Oct 6, 2016

@author: svanhmic
'''
from pyspark import SparkContext
from pyspark.sql import SQLContext
import numpy as np
import re
import matplotlib.pyplot as plt
#from RegnskabsClass import Regnskaber

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

sc = SparkContext("local[8]","PlotingData" )#pyFiles=['/home/svanhmic/workspace/Python/Erhvervs/src/RegnSkabData/RegnskabsClass.py'])
sqlContext = SQLContext(sc)
#sc.addPyFile('/home/svanhmic/workspace/Python/Erhvervs/src/RegnSkabData/RegnskabsClass.py')

def plotFeatureRepresentation(df):
    groupedPivotCount = (df
                       .groupBy()
                       .avg()
                       .collect())
    #print(groupedPivotCount)
    countDict = groupedPivotCount[0].asDict()
    countDict08 = {}
    countDict06 = {}
    countDict04 = {}
    countDict02 = {}
    countDict00 = {}
    
    for (k,v) in countDict.items():
        if v >= 0.8:
            countDict08[k] = v
        elif v >= 0.6 and v < 0.8:
            countDict06[k] = v
        elif v >= 0.4 and v < 0.6:
            countDict04[k] = v
        elif v >= 0.2 and v < 0.4:
            countDict02[k] = v
        else:
            countDict00[k] = v
        #print("keys: " + str(k)+" Values: "+str(v))
    for (k,v) in countDict00.items():
        print("keys: " + str(k)+" Values: "+str(v))
    arrayDict = [countDict00,countDict02,countDict04,countDict06,countDict08]
    intervalArr = [0,0.2,0.4,0.6,0.8]
    
    for (i,val) in enumerate(arrayDict):
        fig = plt.figure(i)
        ax = fig.add_subplot(111)
        ind = np.arange(len(val))
        width = 0.3
        plot1 = ax.barh(ind,val.values(),width,color="green")
        ax.set_xlim(0,(intervalArr[i]+0.21))
        ax.set_yticks(ind+width)
        ytickNames = ax.set_yticklabels([re.sub(r'\W',"",re.sub(r'avg',"", x, )) for x in val.keys()])
        plt.title("Representation of Features with average value above "+str(intervalArr[i]))
        plt.setp(ytickNames, rotation=0, fontsize=10)
    plt.show()

if __name__ == '__main__':
    
    cleanedCsvPath = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/sparkdata/csv"
    dfRegnskabsCount = sqlContext.read.csv(path=cleanedCsvPath+"/pivotRowDataCounts.csv", sep=";", header=True, encoding="utf-8",inferSchema=True)
    #dfRegnskabsCount.show()
    plotFeatureRepresentation(dfRegnskabsCount)