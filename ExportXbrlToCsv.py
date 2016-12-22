'''
Created on Sep 22, 2016

<<<<<<< HEAD
This file contains a generated inherited class, which is created on top of Arelle's Cntlr.Cntlr class 
In order to extract accounts from XBRL-files to csv

=======
>>>>>>> 44c1327efbcacd426e3974403ebc8f9a30b76a07
@author: svanhmic
'''
import sys
sys.path.insert(0, '/home/svanhmic/Programs/Arelle')
from arelle import Cntlr
from arelle import ViewFileFactList
from arelle import ValidateUtr
from arelle import ModelManager

class extractXbrlToCsv(Cntlr.Cntlr):
<<<<<<< HEAD
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
    def __init__(self,inFile,outFile="/tmp/foo.csv", outputColumns=["Name","Dec","Prec","Lang","unitRef","contextRef","EntityIdentifier","Period","Value","Dimensions"]):
        # initialize superclass with default file logger
        super().__init__(logFileName="/tmp/arellelog.txt", logFileMode="w")
        self.outputFile = outFile
        self.outputCols = outputColumns
        self.inputFile = inFile
=======

    # init sets up the default controller for logging to a file (instead of to terminal window)
    def __init__(self,infile,OutFile="/tmp/foo.csv", outputColumns=["Name","Dec","Prec","Lang","unitRef","contextRef","EntityIdentifier","Period","Value","Dimensions"]):
        # initialize superclass with default file logger
        super().__init__(logFileName="/tmp/arellelog.txt", logFileMode="w")
        self.outputFile = OutFile
        self.outputCols = outputColumns
        self.inputFile = infile
>>>>>>> 44c1327efbcacd426e3974403ebc8f9a30b76a07
        
    def run(self):
        # create the modelXbrl by load instance and discover DTS
        #cntlr = Cntlr.Cntlr()
<<<<<<< HEAD
        # select validation of calculation linkbase using infer decimals option            
=======
>>>>>>> 44c1327efbcacd426e3974403ebc8f9a30b76a07
        modlManager = self.modelManager#ModelManager.initialize(cntlr)
        modlManager.validateCalcLB = True
        modlManager.validateUtr = True
        modlManager.validateUtr = True
        modlManager.validateInferDecimals = True
        modlManager.validate()
<<<<<<< HEAD
        
        modelXbrl = modlManager.load(self.inputFile)
        # perform XBRL 2.1, dimensions, calculation
=======
        modelXbrl = modlManager.load(self.inputFile)
        
        # select validation of calculation linkbase using infer decimals option            
#        self.modelManager.validateInferDecimals = True
#        self.modelManager.validateCalcLB = True
#        self.modelManager.validateUtr = True

        # perfrom XBRL 2.1, dimensions, calculation
        #self.modelManager.validate()
>>>>>>> 44c1327efbcacd426e3974403ebc8f9a30b76a07
        ValidateUtr.validateFacts(modelXbrl)
        ViewFileFactList.viewFacts(modelXbrl, self.outputFile, cols=self.outputCols)
        
        # close the loaded instance
        self.modelManager.close()
        
        # close controller and application
        self.close()
            
<<<<<<< HEAD
# if python is initiated as a main_dep program, start the controller
=======
# if python is initiated as a main program, start the controller
>>>>>>> 44c1327efbcacd426e3974403ebc8f9a30b76a07
# if __name__ == "__main__":
#     # create the controller and start it running
#     path = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/finalXML/2014-01-0187139816.xml"
#     extractXbrlToCsv(path).run()