import sys, getopt
import sqlparse
from pandas import *
from os.path import *
from copy import *
from interpreter import *
from do_query import *

# TO DO: later
def doSELECT():
	return 
# WHERE clause does the cartesian product of all relations
def doWHERE(relations,dfs):
	if len(dfs)==1:
		return dfs[0]
	else:
		frame = dfs[0]
		for df in dfs[1:]:
			frame['key']=1
			df['key']=1
			frame = merge(frame,df,on='key')
	return frame



# TO DO: later
def doFROM():
    return 