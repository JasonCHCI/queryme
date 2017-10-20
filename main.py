import sqlparse 
from function import *
from pandas import *
from os.path import *
import sys, getopt

#def parse(arg):

def main(argv):
	inputfile = ''
	try:
		opts, args = getopt.getopt(argv,"hi:",["input"])
	except getopt.GetoptError:
		print 70*"-"
		print "| python main.py -i <inputfile> [other option] [SELECT_statement]"
		print "| "
		print "| Example: $ python main.py -i student.csv 'select * from student'"
		print 70*"-"
		sys.exit(2)
	for opt, arg in opts:
		if opt =='-h':
			print 70*"-"
			print "| python main.py -i <inputfile> [other option] [SELECT_statement]"
			print "| "
			print "| Example: $ python main.py -i student.csv 'select * from student'"
			print 70*"-"
			sys.exit()
		elif opt in ("-i", "--input"):
			inputfile = arg

	return inputfile,args

if __name__=="__main__":
	file,args = main(sys.argv[1:])
	
	if not isfile(file):
		print 70*"-"
		print "| The csv file does not exist. Please input correct one."
		print "| python main.py -i <inputfile> [other option] [SELECT_statement]"
		print 70*"-"
		sys.exit()
	if not args:
		print "read input file:",file,", but cannot read query statement"

	else:
		stm = args[0]



