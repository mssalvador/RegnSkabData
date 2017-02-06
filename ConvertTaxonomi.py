'''
Created on Sep 14, 2016

@author: svanhmic
'''
import re
import os 
import fileinput
import gzip
import multiprocessing
import sys
from shutil import copyfile
import  ExportXbrlToCsv as exp
import GetContexts
#sys.path.insert(0, "/home/biml/Arelle") # inserts Arelle to the pythonpath, apperently
sys.path.insert(0, "/home/svanhmic/Programs/Arelle") # inserts Arelle to the pythonpath, apperently

USER = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/"
ClusterUSER = "/home/biml/bigdata/data_files/regnskaber/"
PATH = USER+"testXML"
NEWPATH = USER+"cleanXML"
TAXPATH = USER+"tax"
ZIPFLES = USER+"testZipped"
CSVFILES = USER+"cleanCSV"
TAXDICT = {}
TAXDICT["20120101"] = "/dcca20120101"
TAXDICT["20121001"] = "/XBRL20121001"
TAXDICT["20130401"] = "/XBRL20130401"
TAXDICT["20140701"] = "/XBRL20140701" 
TAXDICT["20151001"] = "/XBRL20151001"
TAXDICT["20111220"] = "/XBRL20111220_IFRS"
TAXDICT["20131220"] = "/XBRL20131220_IFRS"
TAXDICT["20141220"] = "/XBRL20141220_IFRS"

def acessFolder(parrentInputFolder, parrentOutputFolder, dictTaxonomy, taxonomyPath, remOld=False):
    '''
    This method opens the parrent folder containing all subfolders with xml data
    '''
    
    for l in os.listdir(parrentInputFolder):
        subFolder = parrentInputFolder+"/"+l
        try:
            os.mkdir(parrentOutputFolder+"/"+l)#create emptyoutput subfolders
        except FileExistsError:
            print("folder is there all ready")
        
        acessFiles(subFolder, taxonomyPath, dictTaxonomy, parrentOutputFolder+"/"+l, remOld)

def acessFiles(path,taxpath,taxdict,newFolder,removeOld = True):
    '''
    Opens the xbrl file and adds the taxonomy. The file path and taxonomy is saved into taxlist.csv.
    altered xbrl files with taxonomy is saved in newfolder
    '''
    files = os.listdir(path)
    taxonomyList = []
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
    
    #gets the folders with all dates, zip files are in the folders

    #print files[0:10]
    for folder in os.listdir(location):
        try: 
            os.mkdir(newLoc+"/"+folder)
        except FileExistsError:
            print("folder "+str(folder)+" exists") 
            
        for file in os.listdir(location+"/"+folder):            
            xmlFile = re.sub("gz", "xml", file)
            print(xmlFile)
            with gzip.open(location+"/"+folder+"/"+file,"rt") as zippedFile:
                with open(newLoc+"/"+folder+"/"+xmlFile, "w+") as f:
                    for l in zippedFile.read():
                        f.write(l)
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

    try:
        if ".csv" in infile:
            dateFolder = re.search(r"\d{4}\-\d{2}\-\d{2}", infile)
            #print(dateFolder.group(0))
            copyfile(infile,outfile+dateFolder.group(0)+".csv")
            print(infile+" is a csv file") 
        elif os.path.isfile(outfile) is False:
            exp.extractXbrlToCsv(infile, outfile).run()
            #postProcessing(infile,outfile) # Can't figure this out, very annoying 
            print(str(os.getpid())+", File: "+infile+", is Done!")

        else:
            print(outfile+": is already created!")

    except:
        print("Unexpected error:", sys.exc_info()[0])
    
        print("infile: "+infile)
        print("outfile: "+outfile)
        raise     
        
          
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
            print(csvPath+" the file is already processed\n")
            
def convertFromXmlToCsv(parrentXMLFolder,parrentCSVFolder):
    '''
    Creates a tuple containing folder paths in order to create the appropriate csv files
    '''
    subFiles = os.listdir(parrentXMLFolder)
    allFiles = []
    for subF in subFiles:
        #print(subF)
        allFiles += [[parrentXMLFolder+"/"+subF+"/"+f,parrentCSVFolder+"/"+f+".csv"] for f in os.listdir(parrentXMLFolder+"/"+subF)]
    #for f in fileTuple:
    #    print(f)
    return allFiles
    
if __name__ == '__main__':
    unZipCollection(ZIPFLES, PATH)
    acessFolder(PATH,NEWPATH,TAXDICT,TAXPATH)
    files = tuple(convertFromXmlToCsv(NEWPATH,CSVFILES))
    
    print(len(files))
    #The conversion takes place here
    pool = multiprocessing.Pool(processes=4)
    pool.map(parallelToCsvFromXmlApiStyle,files)
    
    print(len(files))
    #print(len(os.listdir(CSVFILES)))
    
    #csvFiles = os.listdir(CSVFILES)
    #allFiles = tuple([[NEWPATH+"/"+re.sub(r"\.csv","",f),CSVFILES+"/"+f] for f in csvFiles])
    #print(allFiles[:10])
    
    #Units and References are added to the csv-files 
    for file in files:
        print(file[0],"\t",file[1])
        postProcessing(file[0],file[1])
    
    