'''
Created on Sep 22, 2016

This file contains a generated inherited class, which is created on top of Arelle's Cntlr.Cntlr class 
In order to extract accounts from XBRL-files to csv

@author: svanhmic
'''
import sys  
import getpass

if getpass.getuser() == "biml":
    sys.path.insert(0, '/home/biml/Arelle')
elif getpass.getuser() == "svanhmic":
    sys.path.insert(0,"/home/svanhmic/Programs/Arelle")
    

from arelle import Cntlr, FileSource, ViewFileFactList, ValidateUtr, ModelManager
from io import StringIO
from contextlib import redirect_stdout

class extractXbrlToCsv():#Cntlr.Cntlr):

    '''
        Class on top of Cntlr.Cntlr, such that a xbrl file can be convert to a csv file via Arelle's api.
        
        Constructor:
            extractXbrlToCsv(inFile,outFile,outputColumns=["Name","Dec","Prec","Lang","unitRef","contextRef","EntityIdentifier","Period","Value","Dimensions"])
            sets the input and output files in the Cntlr class
        Input:
            inFile - input file e.g. /randomFolder/xml/something.xml
            outFile - output csv file e.g. /randomFolder/csv/something.csv
            outputColumns - the columns in the output that is contained in csv file. Default is: Name, Dec, Prec, Land, unitRef, contextRef, EntityIdentifier, Period, Value,
            Dimensions.
            
        Methods:
            run - The method that converts xbrl to a csv file
    '''
    # init sets up the default controller for logging to a file (instead of to terminal window)
    def __init__(self,infile,OutFile="/tmp/foo.csv", outputColumns=["Name","Dec","Prec","Lang","unitRef","contextRef","EntityIdentifier","Period","Value","Dimensions"]):
        # initialize superclass with default file logger
        #super().__init__(logFileName="/tmp/arellelog.txt", logFileMode="a")
        self.outputFile = OutFile
        self.outputCols = outputColumns
        self.inputFile = infile
        
        #self.logHandler("/tmp/arellelog.txt")
    
    def __del__(self):
        class_name = self.__class__.__name__
        #self.close(saveConfig=False)
        #print(class_name, "destroyed")
        
        
    def run(self):
        # create the modelXbrl by load instance and discover DTS
        
        with StringIO() as arelleLog:
            cntlr = Cntlr.Cntlr()
            with redirect_stdout(arelleLog):
                cntlr.startLogging(logFileName="logToPrint", 
                                  logFormat="[%(messageCode)s] %(message)s - %(file)s",
                                  logLevel="DEBUG",
                                  logToBuffer=False)
        
                # select validation of calculation linkbase using infer decimals option            
                file = FileSource.FileSource(self.inputFile,cntlr)
                modlManager = ModelManager.initialize(cntlr)#self.modelManager#
                modlManager.validateCalcLB = True
                modlManager.validateUtr = True
                modlManager.validateUtr = True
                modlManager.validateInferDecimals = True
                modlManager.validate()
    
                # perform XBRL 2.1, dimensions, calculation
        
                modelXbrl = modlManager.load(file,cntlr)
        
                # perfrom XBRL 2.1, dimensions, calculation
                #self.modelManager.validate()
                ValidateUtr.validateFacts(modelXbrl)
                ViewFileFactList.viewFacts(modelXbrl, self.outputFile, cols=self.outputCols)
                
                # close the loaded instance
                #modlManager.close()
                #self.modelManager.close()
                #modelXbrl.close()
                # close controller and application
                #super().close()
                #self.close()
                #del(self)
            
# if python is initiated as a main_dep program, start the controller
if __name__ == "__main__":
    # create the controller and start it running
    path = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/cleanXML/2014-01-04/2012-01-0133061544.xml"
    extractXbrlToCsv(path,OutFile="/tmp/foo2.csv").run()