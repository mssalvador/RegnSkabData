'''
Created on Jun 17, 2016

@author: svanhmic
'''

import urllib2
import gzip
import json
import requests

regn = "http://regnskaber.virk.dk/10275523/ZG9rdW1lbnRsYWdlcjovLzAzLzIxLzQ3L2NkLzFkLzFlYmItNDZlMi1iNDRiLTNlMGUxZDA3ZjJkNQ.xml"
index = "http://distribution.virk.dk/offentliggoerelser/_search"
#jList = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/regnskab.json"
dataFolderZip ="/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/zipped"
dataFolderXml = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/xml"
site = "http://distribution.virk.dk/offentliggoerelser"

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

def parseToXmlData(jData):
    '''
        takes a json file and extracts the attachments
    '''

    dokData = jData["hits"]["hits"]
    xmlDok = []
    for i in range(0,len(dokData)):
        #print dokData[i]["_source"]["cvrNummer"]
        dokList = dokData[i]["_source"]["dokumenter"]#[0]["dokumentUrl"]
        for d in dokList:
            if d["dokumentMimeType"] == "application/xml":
                x =urllib2.urlopen(d["dokumentUrl"])
                
                text_file = open(dataFolderZip+"/output"+str(dokData[i]["_source"]["cvrNummer"])+"-"+str(dokData[i]["_source"]["regnskab"]["regnskabsperiode"]["startDato"])+".gz", "w+")
                text_file.write(x.read())
                text_file.close()
                
                with gzip.open(dataFolderZip+"/output"+str(dokData[i]["_source"]["cvrNummer"])+"-"+str(dokData[i]["_source"]["regnskab"]["regnskabsperiode"]["startDato"])+".gz", "rb") as f:
                    file_content = f.read()    
                text_file = open(dataFolderXml+"/cvr"+str(dokData[i]["_source"]["cvrNummer"])+"-"+str(dokData[i]["_source"]["regnskab"]["regnskabsperiode"]["startDato"])+".xml", "w+")
                text_file.write(file_content)
                text_file.close()
                xmlDok.append(d["dokumentUrl"])
    for i in xmlDok:
        print i
    print "number of xml-Documents collected is: ", len(xmlDok)
    
if __name__ == '__main__':
    jData = getQueryData("2014-02-01","2014-02-27",1000)
    response = requests.get(index, data=json.dumps(jData),
                                 headers=HEADERS_FOR_JSON)
    response_data = response.json()
    parseToXmlData(response_data)    
