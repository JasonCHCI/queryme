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
        # attrs: attribute in SELECT clause
        # relations: tables name in FROM clause
        # conds: conditions in WHERE clause
        goodstm,attrs,fromClause,conds = checkStatement(stm)

        schemas = {}
        panel = {}

        panel, schemas = parseFrom(fromClause, panel, schemas)




        # query is condition in WHERE clause as a list
        query = checkConditions(conds,fromClause,schemas,panel)

        #createIndex(query, panel, relations)

        print "---------------------------------------------"
        print "| Querying..."
        print "---------------------------------------------"
        start_time = time.time() #used for running time



        # attrs: attributes in SELECT clause
        # relations: tables name in FROM clause
        # query: condition in WHERE clause as a list
        where_df = doWHERE(query,panel,fromClause)
        select_df = doSELECT(where_df, attrs)
        print select_df




        #total_time = time.time()-start_time #total running time, in seconds
        print "---------------------------------------------"
        print "| running time:",time.time()-start_time,"seconds     |"
        print "---------------------------------------------"













