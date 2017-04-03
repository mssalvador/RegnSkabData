'''
Created on Apr 3, 2017

@author: svanhmic
'''
import os
import sys


if __name__ == '__main__':
    
    try:
        path = sys.argv[1]
        csvPath = sys.argv[2]
    except: 
        xmlPath = "/home/biml/bigdata/data_files/regnskaber/cleanXML"
        csvPath = "/home/biml/bigdata/data_files/regnskaber/cleanCSV"
                
    allFolders = os.listdir(xmlPath)
    allFiles = []
    print(os.listdir(xmlPath+"/"+allFolders[0])[:10])
    for folders in allFolders:
        allFiles += os.listdir(xmlPath+"/"+folders)
    print(allFiles[:10])
    
    
    
    try:
        os.mkdir(csvPath+"/odinOnly") # create dir
    except: 
        print("directory already created")
        
    for files in allFiles:
        oldFiles =csvPath+"/"+files+".csv" 
        newFiles = csvPath+"/odinOnly/"+files+".csv"
        os.rename(oldFiles,newFiles)
        
    print("Done!")