'''
Created on Jun 17, 2016

@author: svanhmic
'''

import urllib
import os
import json
import sys
import re
import requests
from datetime import date, timedelta, datetime
from elasticsearch import Elasticsearch
from fileinput import filename
from multiprocessing import Pool

regn = "http://regnskaber.virk.dk/10275523/ZG9rdW1lbnRsYWdlcjovLzAzLzIxLzQ3L2NkLzFkLzFlYmItNDZlMi1iNDRiLTNlMGUxZDA3ZjJkNQ.xml"
index = "http://distribution.virk.dk/offentliggoerelser/_search"
#jList = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/regnskab.json"
site = "http://distribution.virk.dk/offentliggoerelser"
dataFolderXml = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/testXML"
HEADERS_FOR_JSON = {
                    
                    'accept': "application/json; charset=utf-8",
                    'content-type': "application/json"
                    }

#with open(jList) as data_file:    
#    jData = json.load(data_file)

def getQueryData(from_date=None,to_date=None,size=None):
        # Build query
    data = {}
    data['query'] = {}
    if size is not None:
        data['size'] = size
    if from_date is not None or to_date is not None:
        range_value = {}
        if from_date is not None:
            range_value['from'] = from_date
        if to_date is not None:
                range_value['to'] = to_date
        range_value = {
            'offentliggoerelse.offentliggoerelsesTidspunkt':
            range_value
        }
        data['query']['range'] = range_value
    #print data
    return data

def openAndCollectXML(pathList):
    
    
    #print(pathList[2])
    try:
        if pathList[2] is not None:
            #Download the .gz files and saving them as files: date-cvrnumber.gz
            urllib.request.urlretrieve(pathList[2],filename=dataFolderXml+"/"+str(pathList[0])+"/"+str(pathList[1])+".gz")
        else:
            print("Not an adress"+str(pathList[2]))
    except urllib.error.URLError as error:
        print("this failed",error)
        urllib.request.urlcleanup()

    return str(pathList[2])

def extractDokUrl(lst):
    output = list(filter(lambda x: x['dokumentMimeType'] == "application/xml", lst))
    #print(len(output))
    try:
        return output[0]["dokumentUrl"]
    except IndexError:
        #print("No xml file")
        return None

def parseToXmlData(jData):
    '''
        takes a json file and extracts the attachments
    '''
    pool = Pool(processes=4)
    dokData = jData["hits"]["hits"]
    
    #Extract offentliggørelsestidspunkt for the current day
    lf = re.match("\d{4}-\d{2}-\d{2}",dokData[0]["_source"]["offentliggoerelsesTidspunkt"])
    print(lf.group(0)) 
    dokList = [(lf.group(0),str(v["_source"]["regnskab"]["regnskabsperiode"]["startDato"])+str(v["_source"]["cvrNummer"])
                     ,extractDokUrl(v["_source"]["dokumenter"])) for v in dokData]
    
    if os.path.isdir(dataFolderXml+"/"+lf.group(0)) == False:
        os.mkdir(dataFolderXml+"/"+str(lf.group(0)))
    exsistingFiles = os.listdir(dataFolderXml+"/"+str(lf.group(0)))
    updatedDoklist = list(filter(lambda x: x[1]+".gz" not in exsistingFiles,dokList))
    #print(updatedDoklist)
    
    print("number in list: ",len(updatedDoklist))
    #xmlDok = [openAndCollectXML(i)  for i in dokList]
    
    xmlDok = pool.map(openAndCollectXML,updatedDoklist)
    print("number of xml-Documents collected is:  ", len(xmlDok))
    #for i in range(0,len(dokData)):
    #    #print dokData[i]["_source"]["cvrNummer"]
    #    dokList = dokData[i]["_source"]["dokumenter"]#[0]["dokumentUrl"]
    #    for d in dokList:
    #        if d["dokumentMimeType"] == "application/xml":
    #            x = http.urlopen("GET", d["dokumentUrl"])
    #            text_file = open(dataFolderZip+"/"+str(dokData[i]["_source"]["regnskab"]["regnskabsperiode"]["startDato"])+str(dokData[i]["_source"]["cvrNummer"])+".gz", "w+")
    #            text_file.write(str(x.data))
    #            text_file.close()
                
                #with gzip.open(dataFolderZip+"/"+str(dokData[i]["_source"]["regnskab"]["regnskabsperiode"]["startDato"])+str(dokData[i]["_source"]["cvrNummer"])+".gz", "rb") as f:
                #    file_content = f.read()    
                #text_file = open(dataFolderXml+"/"+str(dokData[i]["_source"]["regnskab"]["regnskabsperiode"]["startDato"])+"cvr"+str(dokData[i]["_source"]["cvrNummer"])+".xml", "w+")
                #text_file.write(file_content)
                #text_file.close()
    #            xmlDok.append(d["dokumentUrl"])
    
    #for i in dokList:
    #    print(i)
    
    
if __name__ == '__main__':
    #the main method 
    #usage python3 DownloadData.py startdate enddate pathToSaveFileRootDirectory
    
    if len(sys.argv) == 2:
        start_date = datetime.strptime(sys.argv[1],"%Y-%m-%d")
    elif len(sys.argv) == 3:
        start_date = datetime.strptime(sys.argv[1],"%Y-%m-%d")
        end_date = datetime.strptime(sys.argv[2],"%Y-%m-%d")
    elif len(sys.argv) == 4:
        start_date = datetime.strptime(sys.argv[1],"%Y-%m-%d")
        end_date = datetime.strptime(sys.argv[2],"%Y-%m-%d")
        global dataFolderXml
        dataFolderXml = sys.argv[3]
    else:
        start_date = date(2016,4,1)
        end_date = date(2016,4,2)
    
    d = start_date
    delta = timedelta(days=1)
    while d < end_date:
        sd = d.strftime("%Y-%m-%d")
        d += delta
        ed = d.strftime("%Y-%m-%d")
        jData = getQueryData(sd,ed,1000000)
        response = requests.get(index, data=json.dumps(jData),
                                     headers=HEADERS_FOR_JSON)
        response_data = response.json()
        parseToXmlData(response_data)
        print(sd)    

