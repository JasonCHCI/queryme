import sys, getopt
import sqlparse
from pandas import *
from os.path import *
from copy import *
from interpreter import *
from do_query import *


# TO DO: later
def doSELECT(df, attrs):
    columns = []
    if attrs == '*':
        return df
    else:
        attrs = attrs.split(',')
        for attr in attrs:
            attr = attr.split('.')
            if len(attr) == 2:
                columns.append(attr[1])
            else:
                columns.append(attr[0])
    try:
        df = df[columns]
        return df
    except:
        print "SELECt clause error"


def doFROM(panel):

	return

# WHERE clause does the cartesian product of all relations
def doWHERE(query, panel, relations):
    dfPre = None
    df = None
    temp_panel = {}
    preOP = None
    notOP = None
    final_df = None
    for i in range(len(query)):
        cond = query[i].split()
        if cond[0] in ('AND', 'OR'):
            aoop = cond[0]
            continue
        elif cond[0] == 'NOT':
            noop = cond[0]
            continue
        elif len(cond) == 1:
            tableA, attrA = cond[0].split('.')
            # df = panel[tableA]
            if aoop == 'AND' and tableA in temp_panel:
                df = temp_panel[tableA]
            else:
                df = panel[tableA]
            df = df.query(attrA) if noop <> 'NOT' else df.query(attrA + '== False')  # TO DO
            if aoop == 'OR' and tableA in temp_panel:
                # temp_panel[tableA]=merge(temp_panel[tableA].reset_index(),df.reset_index(),how='outer').set_index(attrA)
                temp_panel[tableA] = temp_panel[tableA].append(df).drop_duplicates()

            else:
                temp_panel[tableA] = df
            # panel[tableA]=df
            final_df = temp_panel[tableA]
            noop = None
        elif len(cond) == 3:
            tableA, attrA = cond[0].split('.')
            tableB, attrB, valueB = None, None, None
            op = cond[1]
            tempB = cond[2].split('.')
            if op == 'LIKE':
                if aoop == 'AND' and tableA in temp_panel:
                    df = temp_panel[tableA]
                else:
                    df = panel[tableA]
                df = doLIKE(df, tempB[0], attrA, noop)
                if aoop == 'OR' and tableA in temp_panel:
                    temp_panel[tableA] = concat([temp_panel[tableA], df]).drop_duplicates()
                else:
                    temp_panel[tableA] = df
                if tableA not in temp_panel:
                    temp_panel[tableA] = df
                # df = doLIKE(df,tempB[0],attrA)
                # panel[tableA]=df
                final_df = temp_panel[tableA]
            else:
                if len(tempB) == 2:
                    tableB, attrB = tempB
                else:
                    valueB = tempB[0]
                # If A is attribute and B is value
                if valueB <> None:
                    if aoop == 'AND' and tableA in temp_panel:
                        df = temp_panel[tableA]
                    else:
                        df = panel[tableA]
                    stm = attrA + op + str(valueB)
                    df = df.query(stm) if noop <> 'NOT' else df.query('not ' + stm)
                    if aoop == 'OR' and tableA in temp_panel:
                        temp_panel[tableA] = concat([temp_panel[tableA], df]).drop_duplicates()
                    else:
                        temp_panel[tableA] = df
                    final_df = temp_panel[tableA]

                # If A and B are all attribute and at the same table
                elif tableA == tableB:
                    if aoop == 'AND' and tableA in temp_panel:
                        df = temp_panel[tableA]
                    else:
                        df = panel[tableA]
                    stm = attrA + op + attrB
                    df = df.query(stm) if noop <> 'NOT' else df.query('not ' + stm)
                    if aoop == 'OR' and tableA in temp_panel:
                        temp_panel[tableA] = concat([temp_panel[tableA], df]).drop_duplicates()
                    else:
                        temp_panel[tableA] = df
                    final_df = temp_panel[tableA]

                # If A and B are all attribute and at different tables
                elif tableB <> None and tableA <> tableB:
                    if aoop == 'AND' and tableA + tableB in temp_panel:
                        df = temp_panel[tableA + tableB]
                    else:
                        dfB = panel[tableB]
                        dfA = panel[tableA]
                        if op == '==' and noop == None:
                            df = merge(dfB, dfA, left_on=attrB, right_on=attrA)
                            temp_panel[tableA + tableB] = concat([temp_panel[tableA + tableB],
                                                                  df]).drop_duplicates() if aoop == 'OR' and tableA + tableB in temp_panel else df
                            temp_panel[tableA] = temp_panel[tableA + tableB]
                            temp_panel[tableB] = temp_panel[tableA + tableB]
                            final_df = temp_panel[tableA + tableB]
                            noop = None
                            continue
                        dfB['key'] = 0
                        dfA['key'] = 0
                        df = merge(dfB, dfA)
                    stm = attrA + op + attrB
                    df = df.query(stm) if noop <> 'NOT' else df.query('not ' + stm)
                    if aoop == 'OR' and tableA + tableB in temp_panel:
                        temp_panel[tableA + tableB] = concat([temp_panel[tableA + tableB], df]).drop_duplicates()
                    else:
                        temp_panel[tableA + tableB] = df
                        temp_panel[tableA] = temp_panel[tableA + tableB]
                        temp_panel[tableB] = temp_panel[tableA + tableB]

                    # stm = attrA+op+'@dfB'+'.'+attrB
                    # df = dfA.query('id == @dfB.id')
                    final_df = temp_panel[tableA + tableB]
            noop = None
    return final_df


def doLIKE(df, b, attA, noop):
    df.reset_index(level=[attA], inplace=True)
    regex_pat = b.replace('%', '.*')
    regex_pat = regex_pat.replace('_', '.')
    if noop <> 'NOT':
        df = df[df[attA].str.match(regex_pat)]
    else:
        df = df[~df[attA].str.match(regex_pat)]
    return df
