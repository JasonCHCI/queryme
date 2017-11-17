import sqlparse
from pandas import *
import numpy as np
from do_query import *
import re
import ast

# @parameter: statement is correct format: SELECT A1,A2,... FROM R1,R2... WHERE C1 AND C2 AND ...
# @return attributes list, tables list, condition list
def parseStatement(statement):
    tokens = re.split('SELECT | FROM | WHERE ', statement)
    if tokens[0]=='': tokens=tokens[1:]
    attrs = [x.strip() for x in tokens[0].split(',')]
    tables= tokens[1].split(',')
    conds = []
    if len(tokens)==3:
        conds = filter(None,re.split('( AND NOT | OR NOT | AND | OR |NOT |\(|\))',tokens[2]))
    return attrs,tables,conds

# @parameter: fileTokens is the list parsed from FROM clause, that stores all csv files we are going to read
# @return: panel that store dataframes, schemas that stores datatypes, tables that stores table names
def readCSVFile(attrs, fileTokens):
    table = []
    panel = {}
    schemas = {}
    for i in range(len(fileTokens)):
        file = fileTokens[i].split()
        df = read_csv(file[0].lstrip(), parse_dates=True, infer_datetime_format=True)
        table_name = file[0].lstrip().split(".")[0]
        if len(file) > 1:
            table_name = file[1].lstrip()
        table.append(table_name)
        schemas[table_name] = {}
        panel[table_name] = df
        for col in df.columns:
            new_col = table_name+'00'+col
            panel[table_name].rename(columns={col:new_col},inplace=True)
            schemas[table_name][new_col] = panel[table_name][new_col].dtype
            if col in attrs:
                attrs.remove(col)
                attrs.append(new_col)
        #print panel[table_name].columns
    for i in range(len(attrs)):
        if len(attrs[i].split('.'))==2:
            attrs[i] = '00'.join(attrs[i].split('.'))

    return attrs, panel, schemas, table

# Conditions should be like A<OP>B, A is an attribute and B can be attribute or value
# @paremeter: conds - condition list in WHERE clause
#             tables- list of tables name
#             schemas - list of schemas
# @return: modified conds
def parseConditions(conds,tables,schemas):
    for i in range(len(conds)):
        cond  = conds[i]
        if cond in (' AND NOT ', ' OR NOT ', 'NOT ', ' AND ', ' OR '):
            conds[i:i + 1] = cond.split()
            continue
        tempc = ''.join(re.findall('"[^"]*"|\'[^\']*\'|[^"\'\s]+',cond)) #remove all empty spaces except string quotes
        tokens = re.split('(<>|>=|<=|=|<|>|LIKE)',tempc) #split condition 'A <op> B' to [A,<op>,B]
        # If condition is single boolean attribute
        if len(tokens)==1 and len(tokens[0].split('.'))==1:
            conds[i]=tokens[0]
            for table in tables:
                for col in schemas[table]:
                    if tokens[0] == col.split('00')[1]:
                        conds[i] = table+'.'+table+'00'+tokens[0]
        # If Condition is A <op> B, tokens=[A,<op>,B]
        elif len(tokens)==3:
            stringA = ''
            stringB = ''
            a = tokens[0].split('.')# attribute A may be 'table.att' or atomic 'att'
            b = tokens[2].split('.')# attribute B may be 'table.att' or atomic 'att' or single value
            op = tokens[1]
            if op == '=': op = '=='
            # if A is attribute,find data type. Otherwise return
            if len(a)==1:
                for table in tables:
                    for col in schemas[table]:
                        if a[0] == col.split('00')[1]:
                            stringA = table + '.' + table+'00'+a[0]
            else:
                stringA = a[0]+'.' + a[0] + '00' + a[1]
            # if B is attribute,find data type. Directly find datatype
            if len(b)==1:
                for table in tables:
                    for col in schemas[table]:
                        if len(col.split('00'))>1 and b[0] == col.split('00')[1]:
                            stringB = table + '.' + table+'00'+a[0]
                else:
                    stringB = b[0]
            else:
                stringB = b[0] + '.' + b[0] + '00' + b[1]
            conds[i] = stringA+' '+op+' '+stringB
    #print conds
    return conds


def createIndex(query,panel,relations):
    dfA = None
    dfB = None
    for i in range(len(query)):
        cond = query[i].split()
        if cond[0] in ('AND','OR','NOT'):
            continue
        else:
            tableA, attrA = cond[0].split('.')
            dfA = panel[tableA]
            if len(cond)==3 and len(cond[2].split('.'))==3:
                tableB, attrB = cond[2].split('.')
                dfB = panel[tableA]
            try:
                dfA.set_index([attrA],append = True, inplace=True)
                if dfB!=None:
                    dfB.set_index([attrB],append = True, inplace=True)
            except:
                pass







