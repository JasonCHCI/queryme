import sys, getopt
import sqlparse
from pandas import *
from os.path import *
from copy import *
from interpreter import *
from do_query import *


# TO DO: later
def doSELECT():
    pass


# TO DO: later
def doFROM(dfs):
    if len(dfs) == 1:
        return dfs[0]
    else:
        frame = dfs[0]
        for df in dfs[1:]:
            frame['key'] = 1
            df['key'] = 1
            frame = merge(frame, df, on='key')
    return frame


# WHERE clause does the cartesian product of all relations
def doWHERE(query, panel, relations):
    dfPre = None
    df = None
    tables = []
    for i in range(len(query)):
        cond = query[i].split()
        if cond[0] in ('AND', 'OR', 'NOT'):
            continue
        elif len(cond) == 1:
            tableA, attrA = cond[0].split('.')
            table = [t for t in tables if tableA in t]
            if table:
                tableA = table[0]
            else:
                tables.append(tableA)
            df = panel["final"]
            df = df[df[attrA]]
            panel[tableA] = df
            print df
        elif len(cond) == 3:
            tableA, attrA = cond[0].split('.')
            table = [t for t in tables if tableA in t]
            if table:
                tableA = table[0]
            else:
                tables.append(tableA)
            tableB, attrB, valueB = None, None, None
            op = cond[1]
            tempB = cond[2].split('.')
            if op == 'LIKE':
                df = panel["final"]
                df = doLIKE(df, tempB[0], attrA)
                panel["final"] = df
                print df
            else:
                if len(tempB) == 2:
                    tableB, attrB = tempB
                    table = [t for t in tables if tableB in t]
                    if table:
                        tableB = table[0]
                    else:
                        tables.append(tableB)
                else:
                    valueB = tempB[0]
                # If A is attribute and B is value
                if valueB <> None:
                    df = panel["final"]
                    stm = attrA + op + str(valueB)
                    df = df.query(stm)
                    panel["final"] = df
                    print df
                # If A and B are all attribute and at the same table
                elif tableA == tableB:
                    df = panel["final"]
                    stm = attrA + op + attrB
                    df = df.query(stm)
                    panel["final"] = df
                    print df
                # If A and B are all attribute and at different tables
                elif tableB <> None and tableA <> tableB:
                    dfA = panel[tableA]
                    dfB = panel[tableB]
                    if attrA == attrB:
                        continue
                    # if op=='==':
                    #	df =
                    # stm = attrA+op+'@dfB'+'.'+attrB
                    # df = dfA.query('id == @dfB.id')
                    print df
        # print panel
        if dfPre is None:
            if df is None:
                print "Not result"
            else:
                dfPre = df
        else:
            if i - 1 >= 0 and query[i - 1] == 'AND':
                # intersect dfPre and df
                pass
            elif i - 1 >= 0 and query[i - 1] == 'OR':
                # union dfPre and df
                pass
        print cond


def doLIKE(df, b, attA):
    regex_pat = b.replace('%', '.*')
    regex_pat = regex_pat.replace('_', '.')
    df = df[df[attA].str.match(regex_pat)]
    return df
