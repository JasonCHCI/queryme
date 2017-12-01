import sys, getopt
import sqlparse
import time
from pandas import *
from os.path import *
from copy import *
from interpreter import *
from do_query import *

#from multiprocessing import Process,Manager


if __name__=="__main__":
    files=[]
    while True:
        print "---------------------------------------------"
        stm = raw_input('| Input a query or input exit:\n'
         '---------------------------------------------\n')
        if stm=='exit':
            sys.exit()

        # get attributes, relations, conditions from statement
        # selectClause: attribute in SELECT clause as a list [a1,a2,a3,...]
        # fromClause: tables name in FROM clause as a list [t1,t2,t3..] or [table1 t1, table2 2, table3 t3,...]
        # whereClause: conditions in WHERE clause as a list [c1,'AND',c2,'OR',C3,...]
        # try:
        selectClause,fromClause,whereClause,distinct = parseStatement(stm)
        # except:
        #     print "Error: parse statement"
        #     sys.exit()

        # panel: a dictionary that stores dataframes
        # schemas: a map that map table to a map which maps attributes to datatype
        # table: a list of table names
        # try:
        selectClause, panel, schemas, tables = readCSVFile(selectClause,fromClause)
        # except:
        #     print "Error: read CSV file"
        #     sys.exit()

        # query: a list of conditions with format of tableName.attrName <op> value or 'AND' or parenthesis
        # attrs: a list of attributes in WHERE clause
        # try:
        query,attrs = parseConditions(whereClause, tables, schemas)
        panel = projection(panel,attrs,selectClause)
        # except:
        #     print "Error: check Condition"
        #     sys.exit()


        #createIndex(query, panel, relations)

        print "---------------------------------------------"
        print "| Querying..."
        print "---------------------------------------------"
        
        start_time = time.time() #used for running time
        #try:
        where_df  = doWHERE(query, panel)
            #print schemas
        # except:
        #     print "Error: querying WHERE clause"
        #     sys.exit()

        select_df = None
        # try:
        select_df = doSELECT(where_df, selectClause,distinct)
        # set_option('display.max_columns', None)
        # set_option('display.max_rows', None)
        # total_time = time.time()-start_time #total running time, in seconds
        print  "Querying finished"
        print "---------------------------------------------"
        print "| Querying time:", time.time() - start_time, "seconds     |"
        print "---------------------------------------------"
        print  "Printing Now..."
        print "---------------------------------------------"

        # except:
        #     print "Error: project SELECT clause"
        #     sys.exit()

        if select_df is not None:
            print select_df














