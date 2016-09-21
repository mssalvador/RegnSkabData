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

def f(x):
    print os.getpid()
    return x


if __name__ == '__main__':
    pool = multiprocessing.Pool(processes=8)
    
    numb = range(100)
    
    print pool.map(f,numb)