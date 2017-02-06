'''
Created on Sep 22, 2016

This file contains a generated inherited class, which is created on top of Arelle's Cntlr.Cntlr class 
In order to extract accounts from XBRL-files to csv

@author: svanhmic
'''
import sys
sys.path.insert(0, '/home/biml/Arelle')
#sys.path.insert(0,"/home/svanhmic/Programs/Arelle")
from arelle import Cntlr
from arelle import ViewFileFactList
from arelle import ValidateUtr
from arelle import ModelManager

class extractXbrlToCsv(Cntlr.Cntlr):

    '''
        Inherited class on top of Cntlr.Cntlr, such that a xbrl file can be convert to a csv file via Arelle's api.
        
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
        super().__init__(logFileName="/tmp/arellelog.txt", logFileMode="a")
        self.outputFile = OutFile
        self.outputCols = outputColumns
        self.inputFile = infile
        
    def run(self):
        # create the modelXbrl by load instance and discover DTS
        #cntlr = Cntlr.Cntlr()

        # select validation of calculation linkbase using infer decimals option            
        modlManager = self.modelManager#ModelManager.initialize(cntlr)
        modlManager.validateCalcLB = True
        modlManager.validateUtr = True
        modlManager.validateUtr = True
        modlManager.validateInferDecimals = True
        modlManager.validate()
    
        # perform XBRL 2.1, dimensions, calculation

        modelXbrl = modlManager.load(self.inputFile)
        
        # select validation of calculation linkbase using infer decimals option            
#        self.modelManager.validateInferDecimals = True
#        self.modelManager.validateCalcLB = True
#        self.modelManager.validateUtr = True

        # perfrom XBRL 2.1, dimensions, calculation
        #self.modelManager.validate()
        ValidateUtr.validateFacts(modelXbrl)
        ViewFileFactList.viewFacts(modelXbrl, self.outputFile, cols=self.outputCols)
        
        # close the loaded instance
        modlManager.close()
        self.modelManager.close()
        modelXbrl.close()
        # close controller and application
        super().close()
        self.close()
            
# if python is initiated as a main_dep program, start the controller
if __name__ == "__main__":
    # create the controller and start it running
    path = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/cleanXML/2014-01-04/2012-01-0133061544.xml"
    extractXbrlToCsv(path,OutFile="/tmp/foo2.csv").run()