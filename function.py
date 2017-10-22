import sqlparse 
from pandas import *

# check if statement is correct format: SELECT A1,A2,... FROM R1,R2... WHERE C1 AND C2 AND ...
def checkStatement(statement):
    tokens = statement.split()
    if all(tokens.count(keyWord)==1 for keyWord in ('SELECT', 'FROM', 'WHERE')):
        iselect = tokens.index('SELECT')
        ifrom = tokens.index('FROM')
        iwhere = tokens.index('WHERE')
        if iselect<ifrom-1 and ifrom<iwhere-1 and iwhere<len(tokens)-1:
            return True, tokens[iselect+1:ifrom],tokens[ifrom+1:iwhere],statement.split('WHERE',1)[1]
    else:
        return False,[],[],''

# check if all attributes, relations/tables exist
def checkExist(attributes,relations,tables,schemas):
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
        return 5
    else:
	   return 3


# TO DO: check if all the attributes in the csv file are type of integer, real, text, date, or boolean
def checkSchema(filename):
    return
    
#def getName(statements):
    

# TO DO:conditions should be like A<OP>B, 
# <OP> is one of =, >, <, <>, >= and <= when A and B are all data types except Boolean
# <OP> is one of AND, OR, NOT when A and B are Boolean. 
# <OP> can be LIKE operator for text
# simple atomic conditions of the form Ai op value, or Ai op Aj, for attributes Ai, Aj and comparison operator op.
def checkConditions(conditions,tables,schemas):
    conds = conditions.split('AND')
    for cond in conds:
        tokens= conds.split()
        a = tokens[0]
        if not any(a in atts for rel,atts in schemas):
            return False
        if len(tokens)==1: return isinstance(tokens)
        if len(tokens)==3:
            first = tokens[0]
            second= tokens [2]
            if tokens[1] in ('=','>','<','<>','>=','<='):
                if not isinstance(first,bool) and not isinstance(second,bool):
                    return True
            elif tokens[1] in ('AND','OR','NOT'):
                if isinstance(first,bool) and isinstance(second,bool):
                    return True

        else:
            return False

# TO DO: later
def doQuery():
	return 


