import sqlparse 
from pandas import *

# TO DO: if file exist, check the file is csv format
def checkFile(filename):
	return

# TO DO: check if all the attributes in the csv file are type of integer, real, text, date, or boolean
def checkSchema(filename):
	return

# TO DO: check if statement is correct format: SELECT A1,A2,... FROM R1,R2... WHERE C1 AND C2 AND ...
def checkStatemet(statement):
	return

# TO DO: check if all attributes, relations/tables exist
def checkExist(statement,filename):
	return

# TO DO:conditions should be like A<OP>B, 
# <OP> is one of =, >, <, <>, >= and <= when A and B are all data types except Boolean
# <OP> is one of AND, OR, NOT when A and B are Boolean. 
# <OP> can be LIKE operator for text
def checkCondition(conditiona):
	return

# TO DO: later
def doQuery():
	return 


