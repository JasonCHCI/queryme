import sqlparse 
from pandas import *
import numpy as np
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

# check if all attributes, relations/tables exist
def checkExist(attributes,relations,tables,schemas):
    attributes=attributes.split(',')
    relations =relations.split(',')
    if all(r in tables for r in relations):
        for a in attributes:
            att=a.split('.')
            if len(att)>2: return 0
            elif len(att)==2: 
                if att[1] not in schemas[att[0]]: return 1
                else: continue
            else:
                if not any(a in schemas[r] for r in relations):
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
def checkConditions(conditions,tables,schemas):
    #conds = conditions.split(' AND ')
    conds = re.split(' AND NOT | OR NOT |NOT | AND | OR ',conditions)
    #print conds
    for cond in conds:
        tempc = ''.join(cond.split())
        operator = ''
        for i in ('<>','>=','<=','=','>','<','LIKE'):
            if i in tempc:
                operator = i 
                tokens = tempc.split(i)#might be attribute and value
                break
        else: 
            a = cond.split('.')
            if len(a)==2 and schemas[a[0]] and a[1] in schemas[a[0]] and schemas[a[0]][a[1]]==np.bool:
                continue
            elif len(a)==1 and any(a[0] in schemas[t] and schemas[t][a[0]]==np.bool for t in tables):
                continue
            else:
                return False
        if operator <> '':
            typeA = None
            typeB = None
            a = tokens[0].split('.')
            b = tokens[1].split('.')
            # if a is relation.attribute,find data type.Otherwise return
            if len(a)==2:
                if schemas[a[0]] and a[1] in schemas[a[0]]:
                    typeA=schemas[a[0]][a[1]]
                    a = a[1]
                else:
                    return False
            # if b is relation.attribute,find data type. Otherwise return
            if len(b)==2:
                if schemas[b[0]] and b[1] in schemas[b[0]]:
                    typeB=schemas[b[0]][b[1]]
                else:
                    return False
            # if a is attribute,find data type. Otherwise return
            if typeA==None:
                a=a[0]
                for table in tables:
                    if a in schemas[table]:
                        typeA = schemas[table][a]
                if typeA==None: 
                    return False
            # if b is attribute,find data type. Directly find datatype
            if typeB==None:
                b=b[0]
                for table in tables:
                    if b in schemas[table]:
                        typeB = schemas[table][b]

            if operator =="LIKE":
             	if not typeA==np.object:
                	return False
                if (not '%' in b) or (not '_' in b):
                	return False
            if operator <>"LIKE" and  typeB<>None and typeB<>typeA:
                 return False
            #if typeB==None:
    return True











