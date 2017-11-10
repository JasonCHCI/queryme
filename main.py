import sys, getopt
import sqlparse
import time
from pandas import *
from os.path import *
from copy import *
from interpreter import *
from do_query import *

#from multiprocessing import Process,Manager

def main(argv):
	files=[]
	try:
		opts, args = getopt.getopt(argv,"i:")

	except getopt.GetoptError:
		print 90*"-"
		print "| python main.py -i <inputfile> [SELECT_statement]"
		print "| "
		print '| Example: $ python main.py -i student.csv "select * from student [WHERE value=\'string\']"'
		print '| note: string value has to be quote by single quotation mark.'
		print 90*"-"
		sys.exit(2)
	for opt, arg in opts:
		if opt == '-i':
			files.append(arg)
		else:
			print 90*"-"
			print "| python main.py -i <inputfile> SELECT_statement]"
			print "| "
			print '| Example: $ python main.py -i student.csv "select * from student [WHERE value=\'string\']"'
			print '| note: string value has to be quote by single quotation mark.'
			print 90*"-"
			sys.exit(2)
	return files,args

if __name__=="__main__":
	start_time = time.time() #used for running time
	files,args = main(sys.argv[1:])
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
		tables ={}
		schemas ={}
		panel = {}
		#panel = Panel(panel)
		for file in files:
			df = read_csv(file,parse_dates=True,infer_datetime_format=True)
			table_name = splitext(file)[0]
			# map table name to file name
			tables[table_name]=file
			# map table name to map2, map2 maps column names to datatype
			schemas[table_name]={}
			# Panel contains multiple tables, map table name to dataframe
			panel[table_name] = df
			#dataframes.append(df) 
			#print df.dtypes
			for col in df.columns:
				dtype = df[col].dtype
				schemas[splitext(file)[0]][col]=df[col].dtype
				#print df[col].dtype
        	if len(df.columns)>300:
        		print 70*"-"
        		print "| The schema of a table should be a set of up to 300 attributes. "
        		print "| Each attribute is one of the following types: Integer, Real, Text, Date, Boolean."
        		print 70*"-"
        		sys.exit()
        

        # get attributes, relations, conditions from statement
        # attrs: attribute in SELECT clause
        # relations: tables name in FROM clause
        # conds: conditions in WHERE clause
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

        # query is condition in WHERE clause as a list
        goodCond, query = checkConditions(conds,tables,schemas,panel)
        if not goodCond:
        	print 70*"-"
        	print "| This conditions are illegal. "
        	print 70*"-"
        	sys.exit()

        createIndex(query,panel, relations)

        # attrs: attributes in SELECT clause
        # relations: tables name in FROM clause
        # query: condition in WHERE clause as a list
        doWHERE(query,panel,relations)

        total_time = time.time()-start_time #total running time, in seconds
        print 'running time:',total_time,'seconds'













