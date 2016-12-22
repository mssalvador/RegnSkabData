'''
Created on Sep 14, 2016

@author: svanhmic
'''
import re
import os 
import fileinput
import gzip
import logging
import multiprocessing
import sys
import io
import GetContexts
import contextlib
from ExportXbrlToCsv import extractXbrlToCsv
sys.path.insert(0, '/home/svanhmic/Programs/Arelle') # inserts Arelle to the pythonpath, apperently

PATH = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/xml"
NEWPATH = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/finalXML"
TAXPATH = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/tax"
ZIPFLES = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/zipped"
CSVFILES = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/testcsv"
TAXDICT = {}
TAXDICT["20120101"] = "/dcca20120101"
TAXDICT["20121001"] = "/XBRL20121001"
TAXDICT["20130401"] = "/XBRL20130401"
TAXDICT["20140701"] = "/XBRL20140701" 
TAXDICT["20151001"] = "/XBRL20151001"
TAXDICT["20111220"] = "/XBRL20111220_IFRS"
TAXDICT["20131220"] = "/XBRL20131220_IFRS"
TAXDICT["20141220"] = "/XBRL20141220_IFRS"


def acessFiles(path,taxpath,taxdict,newFolder,removeOld = True):
    '''
    Opens the xbrl file and adds the taxonomy. The file path and taxonomy is saved into taxlist.csv.
    altered xbrl files with taxonomy is saved in newfolder
    '''
    files = os.listdir(path)
    taxonomyList = []
    taxonomyDict = {}
    for t in taxdict.items():
        taxonomyList = taxonomyList+[taxpath+str(t[1])+"/"+str(t[0])+"/"+str(w) for w in os.listdir(taxpath+str(t[1]))]
    finalTaxList = []
    for t in taxonomyList:
        if t.find(".xsd") >= 0:
            finalTaxList.append(t)
            #print t
    taxonomyFile = open(newFolder+str("/taxlist.csv"),"w+")
    for file in [path+"/"+f for f in files]:
        newLocation = re.sub(path,newFolder,file)
        with open(newLocation,"w+") as newFile:
            for line in fileinput.input(file,backup=file+".bak"):
                cleaned = line
                theString = re.search("xlink:href=.*\.xsd.", line)
                if theString:
                    #print theString.group()             
                    matched = re.search(r'\w+\d+', theString.group())
                    if re.search("\d+/ifrs/",theString.group()):
                        #print("here's an IFRS")
                        notMatched = re.search(r'(\d+/ifrs/\w+.*)',theString.group())
                        #print(notMatched.group())
                    else:
                        notMatched = re.search(r'(\d+/\w+)\.\w+', theString.group())
                    
                    if matched and str(matched.group()).replace(" ","") in taxdict.keys() and notMatched:
                        #print taxpath+taxdict[matched.group()]+"/"+notMatched.group()
                        #print line
                        cleaned = re.sub(r"xlink:href=.*.xsd.","xlink:href=\""+taxpath+taxdict[matched.group()]+"/"+notMatched.group()+"\"",line.rstrip())
                        #print(notMatched.group())
                        #print cleaned
                    else:
                        print(matched.group() ," does not exists as a taxonomy in xml-file: \n" , file)
                        print(notMatched.group())
                        if str(matched.group()).replace(" ","") in taxdict.keys():
                            print(taxdict[str(matched.group()).replace(" ","")])
                    taxonomyFile.write(file+";"+str(matched.group())+"\n")
                newFile.write(cleaned)
            #os.rename(newFile.name,re.sub("testXML", "finalXML",newFile.name))       
        #print "NEWLINE"
        if removeOld:
            os.remove(file)
    taxonomyFile.close()
    print("Inserted taxonomy done!")
            
def unZipCollection(location,newLoc):
    files = os.listdir(location)
    #print files[0:10]
    for file in files:
        xmlFile = re.sub("gz", "xml", file)
        print(xmlFile)
        with gzip.open(location+"/"+file,"rb") as zippedFile:
            file_content = zippedFile.read()
        text_file = open(newLoc+"/"+xmlFile, "w+")
        text_file.write(file_content)
        text_file.close()
    print("Unzipping done!")
    
    
def toCSVFromXML(path,csvDir):
    files = os.listdir(path)
    print(os.getcwd())
    os.chdir("/home/svanhmic/Programs/Arelle")
    for f in files:
        cmdstr = "python3 arelleCmdLine.py -f"+ path+"/"+f+" --facts "+csvDir+"/"+f+".csv --factListCols Name,Dec,Prec,Lang,unitRef,contextRef,EntityIdentifier,Period,Value,Dimensions"
        os.system(cmdstr)
    print(os.getcwd())
    print("csv-transformation done!")
    
def parallelToCsvFromXmlApiStyle(inoutFile):
    infile = inoutFile[0] # the xmlfile
    outfile = inoutFile[1] # the csv file output
    if os.path.isfile(outfile) is False:
        extractXbrlToCsv(infile, outfile).run()
        #postProcessing(infile,outfile) # Can't figure this out, very annoying 
        print(str(os.getpid())+", File: "+infile+", is Done!")
    else:
        print(outfile+": is already created!")
        
        
def postProcessing(docPath,csvPath,logFile="/tmp/log.txt"):
    """ 
    Wrapper for replaceUnitsAndContexts such that a naive "version" control can be made and parallel processing can be initiated.
    
    Input
        docPath: The path to the directory where the xml-files are stored. 
        csvPath: The path to the directory where the csvfiles are stored.
        checkFile: A file that monitors if a file has been updated with context and unitRefs
    
    Output
    """
    with open(logFile,"a") as log:
        try: 
            log.write(GetContexts.replaceUnitsAndContexts(docPath,csvPath)+"\n")
        except KeyError:
            print(csvPath+"the file is already processed\n")

    
if __name__ == '__main__':
    #unZipCollection(ZIPFLES, PATH)
    #acessFiles(PATH,TAXPATH,TAXDICT,NEWPATH,removeOld=False)
    files = os.listdir(NEWPATH)
    files = tuple([[NEWPATH+"/"+f,CSVFILES+"/"+f+".csv"] for f in files]) 
    #The conversion takes place here
    pool = multiprocessing.Pool(processes=8)
    pool.map(parallelToCsvFromXmlApiStyle,files[:20000])
    
    csvFiles = os.listdir(CSVFILES)
    allFiles = tuple([[NEWPATH+"/"+re.sub(r"\.csv","",f),CSVFILES+"/"+f] for f in csvFiles])
    print(allFiles[:10])
    
    #Units and References are added to the csv-files 
    for file in allFiles:
        postProcessing(file[0],file[1])
    
    