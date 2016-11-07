'''
Created on Sep 28, 2016

@author: svanhmic
'''

import csv
import sys

class Regnskaber(object):
    '''
    classdocs
        This class contains the data for a account(regnskab)
        
    '''

    def __init__(self, csvFile):
        '''
        Constructor
        '''
        
        self.file = csvFile # Path to the file 
        self.field = [] #the Fields kept in an array
        self.createField()
    
    def createField(self):
        with open(self.file,'r') as f:
            doc = csv.reader(f,delimiter=",")
            for fields in doc:
                try:
                    self.field.append(Fields(fields[0],fields[1],fields[2],fields[3],fields[4],fields[5],fields[6],fields[7],fields[8],fields[9],fields[10]))
                except IndexError:
                    #print("The last index is not included")
                    self.field.append(Fields(fields[0],fields[1],fields[2],fields[3],fields[4],fields[5],fields[6],fields[7],fields[8],fields[9]))

class Fields(object):
    '''
    classdocs ['\ufeffName', 'Dec', 'Prec', 'Lang',"unitRef","contextRef", 'EntityIdentifier', 'Start', 'End/Instant', 'Value', 'Dimensions']
    '''
     
    def __init__(self,name,decimals,precision,lang,unit,contextRef,id,startDate,endDate,value,dim=None):
        self.name = name
        self.decimals = decimals
        self.precision = precision
        self.lang = lang
        self.unit = unit
        self.contextRef = contextRef
        self.id = id
        self.startDate = startDate
        self.endDate = endDate
        self.value = value
        self.dim = dim
        
    def printIdVal(self):
        print("Id: "+str(self.id)+", value: "+str(self.value))
#         