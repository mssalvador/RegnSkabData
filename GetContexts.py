'''
Created on Oct 27, 2016

@author: svanhmic
@license: Apache 2.0
@summary: Contains methods for extracting context and unit references
'''

import re
import os 
import fileinput
import gzip
import csv
import sys
import getpass
from xml.dom import expatbuilder


if getpass.getuser() == "biml":
    sys.path.insert(0, "/home/biml/Arelle") # inserts Arelle to the pythonpath, apperently
    USER = "/home/biml/bigdata/data_files/regnskaber/"
elif getpass.getuser() == "svanhmic":
    sys.path.insert(0, "/home/svanhmic/Programs/Arelle") # inserts Arelle to the pythonpath, apperently
    USER = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/"


CSVFILES = USER+"cleanCSV"
NEWPATH = USER+"cleanXML"
       

def getContextRef(docPath):
    """ 
    Extrats context references fra an xml document, and returns a dictionary with contexts
    
    Input
        docPath: The path to the diretory where the files are stored. 
    
    Output
        contexDic: A dictionary with contextsrefs as index and values as the translation
    """
    
    try:
        xbrlDok = expatbuilder.parse(docPath, False)
        #print(xbrlDok)
        #print(xbrlDok.documentElement)
        pref =""
        if xbrlDok.documentElement.tagName != xbrlDok.documentElement.localName:
            pref = str(re.match("\w+:", xbrlDok.documentElement.tagName).group())
              
        contexts = xbrlDok.documentElement.getElementsByTagName(pref+"context")
        contextDic = {}
        for i in contexts:
            contextDic[i.getAttribute("id")] = i.getElementsByTagName(pref+"identifier")[0].firstChild.nodeValue
            #print(i.getAttribute("id"))
            #print(i.getElementsByTagName(pref+"identifier")[0].firstChild.nodeValue)
        #print(contexts)
        return contextDic
    except:
        print("Well this is embarresing, contexts are not present in: "+docPath)
        return None
    
def getUnitRef(docPath):
    """ 
    Extrats unit references fra an xml document, and returns a dictionary with contexts
    
    Input
        docPath: The path to the diretory where the files are stored. 
    
    Output
        unitDic: A dictionary with contextsrefs as index and values as the translation
    """
    
    try:
        unitDic = {}
        xbrlDok = expatbuilder.parse(docPath, False)
        pref=""
        if xbrlDok.documentElement.tagName != xbrlDok.documentElement.localName:
            pref = str(re.match("\w+:", xbrlDok.documentElement.tagName).group())
        units = xbrlDok.documentElement.getElementsByTagName(pref+"unit")
        for i in units:
            unitDic[i.getAttribute("id")] = i.getElementsByTagName(pref+"measure")[0].firstChild.nodeValue
        return unitDic
    except:
        print("Well this is embarresing, units are not present in:"+docPath)
        return None
    
def replaceUnitsAndContexts(docPath,csvPath):
    """ 
    Transforms a context- and unit-references in csv file, such the real units and contexts are saved in the csv-file. 
    
    Input
        docPath: The path to the directory where the xml-files are stored. 
        csvPath: The path to the directory where the csvfiles are stored.
    
    Output
        contexDic: A dictionary with context refs as index and values as the translation
    """
    
    unitDict = getUnitRef(docPath) # Get unit references for document
    contextDict = getContextRef(docPath) # Get Context references for document
    #print(contextDict)
    try:
        newRows = []
        fieldNames = [] 
        with open(csvPath) as csvfile:
            file = csv.DictReader(csvfile,delimiter="|",dialect='excel')
            #fieldNames = [fieldName.decode("ascii").encode("utf-8") for fieldName in file.fieldnames]
            fieldNames = file.fieldnames
            #print(fieldNames)

            for row in file:
                if row["unitRef"] != "":
                    if row["unitRef"] == None:
                        print(row["unitRef"])
                    row["unitRef"] = unitDict[row["unitRef"]]      
                if row["contextRef"] != "":
                    if row["contextRef"] == None:
                        print(row["contextRef"])
                    row["contextRef"] = contextDict[row["contextRef"]]
                if None in row.keys():
                    #print(row.keys())
                    secondDim = [row["Dimensions"],row[None][0]]
                    #print(secondDim)
                    row["Dimensions"] = secondDim
                    del row[None]
                newRows.append(row)
        with open(csvPath,"w+") as outputcsv:
            outputFile = csv.DictWriter(outputcsv,fieldnames=fieldNames,delimiter="|",quoting=csv.QUOTE_ALL,dialect='excel')
            outputFile.writeheader()
            outputFile.writerows(newRows)
        return str(csvPath)+": OK"
    except TypeError as te:
        print(str(te))
        #print(row["contextRef"])
        print("contexts: ",contextDict)
        print("units: ",unitDict)
        return str(csvPath)+str(contextDict)+str(unitDict)
    except OSError as systemstuff:
        print(systemstuff.strerror)
        #print("the file was not found")
        print(systemstuff.filename)
        return str(csvPath)+":Not Found"
    
        
        
def postProcessing(docPath,csvPath,checkFile=None):
    """ 
    Wrapper for replaceUnitsAndContexts such that a naive "version" control can be made and parallel processing can be initiated.
    
    Input
        docPath: The path to the directory where the xml-files are stored. 
        csvPath: The path to the directory where the csvfiles are stored.
        checkFile: A file that monitors if a file has been updated with context and unitRefs
    
    Output
    """
    #with open(checkFile,'w+') as checkList:
    #    checkDict = {}
    #    for line in checkList.read():
    #        print(line)
            
    replaceUnitsAndContexts(docPath,csvPath)  
    
if __name__ == '__main__':
    argsLength = len(sys.argv)
    if argsLength == 1:
        NEWPATH = sys.argv[1]
    elif argsLength == 2:
        NEWPATH = sys.argv[1]
        CSVFILES = sys.argv[2]
    else:
        CSVFILES = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/testcsv"
        NEWPATH = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/testXML"
       
    try:
        files = os.listdir(NEWPATH)
        postProcessing(NEWPATH+"/2011-09-2733961871.xml",CSVFILES+"/2011-09-2733961871.xml.csv")
    except:
        print("none")
    
