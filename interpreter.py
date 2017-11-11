import sqlparse
from pandas import *
import numpy as np
from do_query import *
import re
import ast

# check if statement is correct format: SELECT A1,A2,... FROM R1,R2... WHERE C1 AND C2 AND ...
def checkStatement(statement):
    tokens = statement.split()
    if all(tokens.count(keyWord)==1 for keyWord in ('SELECT', 'FROM')):
        iselect = tokens.index('SELECT')
        ifrom = tokens.index('FROM')
        if tokens.count('WHERE')==1:
            iwhere = tokens.index('WHERE')
            if iselect<ifrom-1 and ifrom<iwhere-1 and iwhere<len(tokens)-1:
                return True, ''.join(tokens[iselect+1:ifrom]),''.join(tokens[ifrom+1:iwhere]),statement.split('WHERE ',1)[1]
        else:
            if iselect<ifrom-1:
                return True, ''.join(tokens[iselect+1:ifrom]),''.join(tokens[ifrom+1:]),''
    return False,'','',''

# check if all attributes in SELECT clause and relations/tables in FROM clause exist in dataframes
def checkExist(attributes,relations,tables,schemas):
    attributes=attributes.split(',')
    relations =relations.split(',')
    if all(r in tables for r in relations):
        # attribute may be format of 'table.att' or 'att'
        for a in attributes:
            att=a.split('.')
            if len(att)>2: return 0
            elif len(att)==2:
                if att[1] not in schemas[att[0]]: return 1
                else: continue
            else:
                if not any(a in schemas[r] for r in relations) and a<>'*':
                    return 2
                elif sum(a in schemas[r] for r in relations)>1:
                    return 4
        return 5
    else:
        return 3


# Conditions should be like A<OP>B, A is an attribute and B can be attribute or value
# <OP> is one of =, >, <, <>, >= and <= when A and B are all data types except Boolean
# <OP> is one of AND, OR, NOT when A and B are Boolean.
# <OP> can be LIKE operator for text
def checkConditions(conditions,tables,schemas,panel):
    conds = re.split('( AND NOT | OR NOT | AND | OR |NOT )',conditions)
    statement = []
    tables = tables.split(',')
    for cond in conds:
        if cond in (' AND NOT ',' OR NOT ','NOT ',' AND ',' OR '):
            statement.extend(cond.split())
            continue
        #tempc = ''.join(cond.split())
        tempc= ''.join(re.findall('"[^"]*"|\'[^\']*\'|[^"\'\s]+',cond))
        operator = ''
        # If condition is 'A <op> B', then split condition 'A <op> B' to [A,B], and operator = <op>
        for i in ('<>','>=','<=','=','>','<','LIKE'):
            if i in tempc:
                operator = i
                tokens = tempc.split(i)#split condition 'A <op> B' to [A,B]
                break
        # If condition is single boolean attribute, test if the attribute exist and if it is boolean value
        else:
            a = cond.split('.')
            if len(a)==2 and schemas[a[0]] and a[1] in schemas[a[0]] and schemas[a[0]][a[1]]==np.bool:
                statement.append(a[0]+'.'+a[1])
                continue
            elif len(a)==1:
                count=0
                for t in tables:
                    if a[0] in schemas[t] and schemas[t][a[0]]==np.bool:
                        tb = t
                        att= a[0]
                        count+=1
                if count==1:
                    statement.append(tb+'.'+att)
                    continue
                else:
                    return False,[]
            else:
                return False,[]
        # if operator is not '', which means tokens is [A,B]
        if operator <> '':
            typeA = None
            typeB = None
            tableA,tableB,attA,attB,valueB= '','','','',None
            a = tokens[0].split('.')# attribute A may be 'table.att' or atomic 'att'
            b = tokens[1].split('.')# attribute B may be 'table.att' or atomic 'att' or single value
            # if A is relation.attribute,find data type.Otherwise return
            if len(a)==2 and schemas[a[0]] and a[1] in schemas[a[0]]:
                typeA=schemas[a[0]][a[1]]
                tableA = a[0]
                attA = a[1]
            # if A is attribute,find data type. Otherwise return
            elif len(a)==1:
                a=a[0]
                count = 0
                for table in tables:
                    if a in schemas[table]:
                        count+=1
                        typeA = schemas[table][a]
                        tableA= table
                        attA = a
                if typeA==None or count<>1:
                    #print "attribute ambiguous"
                	return False,[]
            else:
                return False,[]
            # if B is relation.attribute,find data type. Otherwise return
            if len(b)==2 and schemas[b[0]] and b[1] in schemas[b[0]]:
                typeB=schemas[b[0]][b[1]]
                tableB = b[0]
                attB = b[1]
            # if B is attribute,find data type. Directly find datatype
            elif len(b)==1:
                b=b[0]
                count = 0
                for table in tables:
                    if b in schemas[table]:
                        count+=1
                        typeB = schemas[table][b]
                        tableB= table
                        attB = b
                if count>1: return False,[]
                if count==0:
                    try:
                        valueB = eval(b)
                        tempb = np.array(eval(b))
                        if tempb.dtype.char == 'S':
                            tempb = tempb.astype(np.object)
                        typeB =  tempb.dtype
                    except:
                        return False,[]
            else:
                return False,[]
            # If <op> is LIKE and A and B have datatype of string, we do query
            # Otherwise, return
            if operator =="LIKE":
                if typeA<>np.object and typeA<>typeB:
                    return False,[]
                else:
                    statement.append(tableA+'.'+attA+' '+operator+' '+valueB)
            # If <op> is in '<>','>=','<=','=','>','<' and A and B have the same datatype except Boolean,
            # then we do query
            if operator <>"LIKE":
                if operator=='=':
                    operator='=='
                if typeB==typeA and typeA<>np.bool:
                    if valueB==None:
                        statement.append(tableA+'.'+attA+' '+operator+' '+tableB+'.'+attB)
                    else:
                        statement.append(tableA+'.'+attA+' '+operator+' '+str(b))

                else:
                    return False,[]

    return True,statement





def createIndex(query,panel,relations):
    for i in range(len(query)):
        cond = query[i].split()
        if cond[0] in ('AND','OR','NOT'):
            continue
        else:
            tableA, attrA = cond[0].split('.')
            df = panel[tableA]
            df.set_index([attrA],append = True, inplace=True)






