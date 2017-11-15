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
        print "| Please input a query statement."
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
        print "---------------------------------------------"
        print "| Reading files..."
        print "---------------------------------------------"
        for file in files:
            df = read_csv(file,parse_dates=True,infer_datetime_format=True)
            #df[0:1000000].to_csv("Crimes100.csv")
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

        # while True:
        #     print "---------------------------------------------"
        #     stm = raw_input('| Input a query or input exit:\n'
        #                     '---------------------------------------------\n')
        #     if stm=='exit':
        #         sys.exit()

        # get attributes, relations, conditions from statement
        # attrs: attribute in SELECT clause
        # relations: tables name in FROM clause
        # conds: conditions in WHERE clause
        goodstm,attrs,relations,conds = checkStatement(stm)
        print conds

        parseFrom(relations)


        if not goodstm:
            print 70*"-"
            print "| Please input a query statement with legal format. "
            print '| Format: "SELECT A1,A2,... FROM R1,R2... [WHERE C1 AND C2 AND ...]"'
            print 70*"-"
            sys.exit()



        # query is condition in WHERE clause as a list
        query = checkConditions(conds,relations,schemas,panel)

        #createIndex(query, panel, relations)

        print "---------------------------------------------"
        print "| Querying..."
        print "---------------------------------------------"
        start_time = time.time() #used for running time



        # attrs: attributes in SELECT clause
        # relations: tables name in FROM clause
        # query: condition in WHERE clause as a list
        where_df = doWHERE(query,panel,relations)
        select_df = doSELECT(where_df, attrs)
        print select_df




        #total_time = time.time()-start_time #total running time, in seconds
        print "---------------------------------------------"
        print "| running time:",time.time()-start_time,"seconds     |"
        print "---------------------------------------------"













