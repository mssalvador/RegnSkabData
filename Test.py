'''
Created on Sep 14, 2016

@author: svanhmic
'''

from RegnskabsClass import Regnskaber
import os
import re
<<<<<<< HEAD
import sys
import contextlib 
import io
=======
>>>>>>> 44c1327efbcacd426e3974403ebc8f9a30b76a07
from datetime import datetime
from decimal import Decimal
path = "/home/svanhmic/workspace/Python/Erhvervs/data/regnskabsdata/csv"

files = os.listdir(path)
#regnskab = Regnskaber(files[0])
def mapDouble(*x):
    return [2*v for v in x]

class ProClass:
    
    def __init__(self):
        self._tmp = 100000
        
    @property
    def tmp(self):
        """Check the current temp (getter)"""
        return self._tmp

    @tmp.setter
    def tmp(self,val):
        self._tmp = val
        
    @tmp.deleter
    def tmp(self):
        del self._tmp
<<<<<<< HEAD


def foo():
    print("heeeeeeeeeeeeeej!")   

if __name__ == "__main__":
    
    with open("/tmp/log.txt",'w+') as log:
        with contextlib.redirect_stdout(log):
            foo() 

=======
    

if __name__ == "__main__":
    
    hej = ('unicode','int','float')
    if 'unicode' in hej:
        print("this is good")
    else:
        print("Damn :P")
    #print(int(Decimal("1.0")))
    #print(int(float(re.sub(",",".",re.sub("\.", "", hej)))))
    
    p = ProClass()
    
    print(p.tmp)
    p.tmp = 1
    print(p.tmp)
    del p.tmp
>>>>>>> 44c1327efbcacd426e3974403ebc8f9a30b76a07
