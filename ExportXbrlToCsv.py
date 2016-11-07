'''
Created on Sep 22, 2016

@author: svanhmic
'''
import sys
sys.path.insert(0, '/home/svanhmic/Programs/Arelle')
from arelle import Cntlr
from arelle import ViewFileFactList
from arelle import ValidateUtr
from arelle import ModelManager

class extractXbrlToCsv(Cntlr.Cntlr):

    # init sets up the default controller for logging to a file (instead of to terminal window)
    def __init__(self,infile,OutFile="/tmp/foo.csv", outputColumns=["Name","Dec","Prec","Lang","unitRef","contextRef","EntityIdentifier","Period","Value","Dimensions"]):
        # initialize superclass with default file logger
        super().__init__(logFileName="/tmp/arellelog.txt", logFileMode="w")
        self.outputFile = OutFile
        self.outputCols = outputColumns
        self.inputFile = infile
        
    def run(self):
        # create the modelXbrl by load instance and discover DTS
        #cntlr = Cntlr.Cntlr()
        modlManager = self.modelManager#ModelManager.initialize(cntlr)
        modlManager.validateCalcLB = True
        modlManager.validateUtr = True
        modlManager.validateUtr = True
        modlManager.validateInferDecimals = True
        modlManager.validate()
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
        self.modelManager.close()
        
        # close controller and application
        self.close()
            
# if python is initiated as a main program, start the controller
# if __name__ == "__main__":
#     # create the controller and start it running
#     path = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/finalXML/2014-01-0187139816.xml"
#     extractXbrlToCsv(path).run()