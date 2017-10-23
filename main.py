import sys, getopt
import sqlparse
from pandas import *
from os.path import *
from copy import *
from interpreter import *
from do_query import *

#def parse(arg):

def main(argv):
	files=[]
	try:
		opts, args = getopt.getopt(argv,"i:")

	except getopt.GetoptError:
		print 70*"-"
		print "| python main.py -i <inputfile> [SELECT_statement]"
		print "| "
		print "| Example: $ python main.py -i student.csv 'select * from student'"
		print 70*"-"
		sys.exit(2)
	for opt, arg in opts:
		if opt == '-i':
			files.append(arg)
		else:
			print 70*"-"
			print "| python main.py -i <inputfile> SELECT_statement]"
			print "| "
			print "| Example: $ python main.py -i student.csv 'select * from student'"
			print 70*"-"
			sys.exit(2)
	return files,args

if __name__=="__main__":
	files,args = main(sys.argv[1:])
	tables ={}
	schemas ={}
	if any(not isfile(file) for file in files):
		print 70*"-"
		print "| The file does not exist. Please input correct one."
		print 70*"-"
		sys.exit()
	if any(not file.endswith('.csv') for file in files):
		print 70*"-"
		print "| The file is not csv format. Please input a csv file."
		print 70*"-"
		sys.exit()
	if not args:
		print 70*"-"
		print "| Please input a currect query statement"
		print 70*"-"
		sys.exit()
	if len(args)>1:
		print 70*"-"
		print "| Please input exact one statement"
		print 70*"-"
		sys.exit()
	else:
		stm = args[0]
		for file in files:
			df = read_csv(file,parse_dates=True)
			tables[splitext(file)[0]]=file
			schemas[splitext(file)[0]]={}
			for col in df.columns:
				schemas[splitext(file)[0]][col]=df[col].dtype
				#print df[col].dtype
        	if len(df.columns)>300:
        		print 70*"-"
        		print "| The schema of a table should be a set of up to 300 attributes. "
        		print "| Each attribute is one of the following types: Integer, Real, Text, Date, Boolean."
        		print 70*"-"
        		sys.exit()

        #get attributes, relations, conditions from statement
        goodstm,attrs,relations,conds = checkStatement(stm)
        if not goodstm:
        	print 70*"-"
        	print "| Please input a query statemetn with legal format. "
        	print '| Format: "SELECT A1,A2,... FROM R1,R2... [WHERE C1 AND C2 AND ...]"'
        	print 70*"-"
        	sys.exit()

        exist = checkExist(attrs,relations,tables,schemas)
        if exist==0:
        	print 70*"-"
        	print "| Please input a attribute with legal format. "
        	print "| attribute can be express by 'name' or 'relationName.attributeName'"
        	print 70*"-"
        	sys.exit()
        if exist==1 or exist==2:
        	print 70*"-"
        	print "| This attribute doesn't exist in tables. "
        	print 70*"-"
        	sys.exit()
        if exist==3:
        	print 70*"-"
        	print "| This table doesn't exist. "
        	print 70*"-"
        	sys.exit()
        if exist==4:
        	print 70*"-"
        	print "| This attribute's name is duplicate. "
        	print 70*"-"
        	sys.exit()
        #print attrs,relations,conds
        #print tables,schemas
        #print isinstance(1==2,bool)
        if goodstm and conds<>'' and not checkConditions(conds,tables,schemas):
        	print 70*"-"
        	print "| This conditions are illegal. "
        	print 70*"-"
        	sys.exit()














