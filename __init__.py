"""RegnskabsData"""
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.types import IntegerType,StructType,StringType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors,VectorUDT
import pyspark.sql.functions as F
import os
import re
import numpy as np
import matplotlib.pyplot as plt
#from RegnskabsClass import Regnskaber
# HACK!!!
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

sc = SparkContext("local[8]","FeatureAnalysis" )#pyFiles=['/home/svanhmic/workspace/Python/Erhvervs/src/RegnSkabData/RegnskabsClass.py'])
sqlContext = SQLContext(sc)
#sc.addPyFile('/home/svanhmic/workspace/Python/Erhvervs/src/RegnSkabData/RegnskabsClass.py')