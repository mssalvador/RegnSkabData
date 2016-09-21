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

PATH = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/xml"
NEWPATH = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/finalXML"
TAXPATH = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/tax"
ZIPFLES = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/zipped"
CSVFILES = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/csv"
TAXDICT = {}
TAXDICT["20120101"] = "/dcca20120101"
TAXDICT["20121001"] = "/XBRL20121001"
TAXDICT["20130401"] = "/XBRL20130401"
TAXDICT["20140701"] = "/XBRL20140701" 
TAXDICT["20151001"] = "/XBRL20151001"
TAXDICT["20111220"] = "/XBRL20111220_IFRS"
TAXDICT["20131220"] = "/XBRL20131220_IFRS"

def acessFiles(path,taxpath,taxdict,newFolder,removeOld = True):
    '''
    
    '''
    files = os.listdir(path)
    taxonomyList = []
    taxonomyDict = {}
    for t in TAXDICT.items():
        taxonomyList = taxonomyList+[taxpath+str(t[1])+"/"+str(t[0])+"/"+str(w) for w in os.listdir(taxpath+str(t[1]))]
    finalTaxList = []
    for t in taxonomyList:
        if t.find(".xsd") >= 0:
            finalTaxList.append(t)
            #print t
    
    for file in [path+"/"+f for f in files]:
        newLocation = re.sub(path,newFolder,file)
        with open(newLocation,"w+") as newFile:
            for line in fileinput.input(file,backup=file+".bak"):
                cleaned = line
                theString = re.search("xlink:href=.*\.xsd.", line)
                if theString:
                    #print theString.group()             
                    matched = re.search(r'\w+\d+', theString.group())
                    notMatched = re.search(r'(\d+/\w+)\.\w+', theString.group())
                    if matched and taxdict.has_key(matched.group()) and notMatched:
                        #print taxpath+taxdict[matched.group()]+"/"+notMatched.group()
                        #print line
                        cleaned = re.sub(r"xlink:href=.*.xsd.","xlink:href=\""+taxpath+taxdict[matched.group()]+"/"+notMatched.group()+"\"",line.rstrip())
                        #print cleaned
                    else:
                        print(matched.group() ," does not exists as a taxonomy in xml-file: \n" , file)
                newFile.write(cleaned)
            #os.rename(newFile.name,re.sub("testXML", "finalXML",newFile.name))       
        #print "NEWLINE"
        if removeOld:
            os.remove(file)
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
        cmdstr = "python3 arelleCmdLine.py -f"+ path+"/"+f+" --facts "+csvDir+"/"+f+".csv --factListCols Name,Dec,Prec,Lang,EntityIdentifier,Period,Value,Dimensions"
        os.system(cmdstr)
    print(os.getcwd())
    print("csv-transformation done!")
    
def paralleltoCSVFromXML(file):
    dir = NEWPATH
    csvDir = CSVFILES
    os.chdir("/home/svanhmic/Programs/Arelle")
    cmdstr = "python3 arelleCmdLine.py -f"+ dir+"/"+file+" --facts "+csvDir+"/"+file+".csv --factListCols Name,Dec,Prec,Lang,EntityIdentifier,Period,Value,Dimensions"
    os.system(cmdstr)
    print os.getpid()
    sys.stdout.flush()
    
def info(title):
    print title
    print 'module name:', __name__
    if hasattr(os, 'getppid'):  # only available on Unix
        print 'parent process:', os.getppid()
    print 'process id:', os.getpid()    

  
    
if __name__ == '__main__':
    #unZipCollection(ZIPFLES, PATH)
    #acessFiles(PATH,TAXPATH,TAXDICT,NEWPATH,removeOld=False)
    files = os.listdir(NEWPATH)
    #for f in files:
    pool = multiprocessing.Pool(processes=8)
    pool.map(paralleltoCSVFromXML,files)
        #multiprocessing.log_to_stderr()
        #logger = multiprocessing.get_logger()
        #logger.setLevel(logging.INFO)
        #p = multiprocessing.Process(target=paralleltoCSVFromXML(f,NEWPATH,CSVFILES))
        #p.run()
    #toCSVFromXML(NEWPATH,CSVFILES)
    
    