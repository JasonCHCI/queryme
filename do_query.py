import sys, getopt
import sqlparse
import shlex
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
def doWHERE(query, panel, temp_panel={}, final_df = None):
    preOP = None
    notOP = None
    for i in range(len(query)):
        print "Initial cond:"
        print query[i]
        cond = shlex.split(query[i])
        if cond[0] == '(':
            
        if cond[0] in ('AND', 'OR'):
            preOP = cond[0]
            continue
        elif cond[0] == 'NOT':
            notOP = cond[0]
            continue
        elif len(cond) == 1:
            tableA, attrA = cond[0].split('.')
            # df = panel[tableA]
            if preOP == 'AND' and tableA in temp_panel:
                df = temp_panel[tableA]
            else:
                df = panel[tableA]
            df = df.query(attrA) if notOP <> 'NOT' else df.query(attrA + '== False')  # TO DO
            if preOP == 'OR' and tableA in temp_panel:
                # temp_panel[tableA]=merge(temp_panel[tableA].reset_index(),df.reset_index(),how='outer').set_index(attrA)
                temp_panel[tableA] = temp_panel[tableA].append(df).drop_duplicates()

            else:
                temp_panel[tableA] = df
            # panel[tableA]=df
            final_df = temp_panel[tableA]
            notOP = None
        elif len(cond) == 3:
            tableA, attrA = cond[0].split('.')
            tableB, attrB, valueB = None, None, None
            op = cond[1]
            tempB = cond[2].split('.')
            if op == 'LIKE':
                if preOP == 'AND' and tableA in temp_panel:
                    df = temp_panel[tableA]
                else:
                    df = panel[tableA]
                df = doLIKE(df, tempB[0], attrA, notOP)
                if preOP == 'OR' and tableA in temp_panel:
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
                    if preOP == 'AND' and tableA in temp_panel:
                        df = temp_panel[tableA]
                    else:
                        df = panel[tableA]
                    stm = attrA + op + str(valueB)
                    df = df.query(stm) if notOP <> 'NOT' else df.query('not ' + stm)
                    if preOP == 'OR' and tableA in temp_panel:
                        temp_panel[tableA] = concat([temp_panel[tableA], df]).drop_duplicates()
                    else:
                        temp_panel[tableA] = df
                    final_df = temp_panel[tableA]

                # If A and B are all attribute and at the same table
                elif tableA == tableB:
                    if preOP == 'AND' and tableA in temp_panel:
                        df = temp_panel[tableA]
                    else:
                        df = panel[tableA]
                    stm = attrA + op + attrB
                    df = df.query(stm) if notOP <> 'NOT' else df.query('not ' + stm)
                    if preOP == 'OR' and tableA in temp_panel:
                        temp_panel[tableA] = concat([temp_panel[tableA], df]).drop_duplicates()
                    else:
                        temp_panel[tableA] = df
                    final_df = temp_panel[tableA]

                # If A and B are all attribute and at different tables
                elif tableB <> None and tableA <> tableB:
                    if preOP == 'AND' and tableA + tableB in temp_panel:
                        df = temp_panel[tableA + tableB]
                    else:
                        dfB = panel[tableB]
                        dfA = panel[tableA]
                        if op == '==' and notOP == None:
                            print panel[tableA]
                            df = merge(dfB, dfA.dropna(), left_on=attrB, right_on=attrA)
                            print panel[tableA]
                            temp_panel[tableA + tableB] = concat([temp_panel[tableA + tableB],
                                                                  df]).drop_duplicates() if preOP == 'OR' and tableA + tableB in temp_panel else df
                            temp_panel[tableA] = temp_panel[tableA + tableB]
                            temp_panel[tableB] = temp_panel[tableA + tableB]
                            final_df = temp_panel[tableA + tableB]
                            notOP = None
                            continue
                        dfB['key'] = 0
                        dfA['key'] = 0
                        df = merge(dfB, dfA)
                    stm = attrA + op + attrB
                    df = df.query(stm) if notOP <> 'NOT' else df.query('not ' + stm)
                    if preOP == 'OR' and tableA + tableB in temp_panel:
                        temp_panel[tableA + tableB] = concat([temp_panel[tableA + tableB], df]).drop_duplicates()
                    else:
                        temp_panel[tableA + tableB] = df
                        temp_panel[tableA] = temp_panel[tableA + tableB]
                        temp_panel[tableB] = temp_panel[tableA + tableB]

                    # stm = attrA+op+'@dfB'+'.'+attrB
                    # df = dfA.query('id == @dfB.id')
                    final_df = temp_panel[tableA + tableB]
            notOP = None
    return final_df


def doLIKE(df, b, attA, noop):
    #df.reset_index(level=[attA], inplace=True)
    regex_pat = b.replace('%', '.*')
    regex_pat = regex_pat.replace('_', '.')
    if noop <> 'NOT':
        df = df[df[attA].str.match(regex_pat, na=False)]
    else:
        df = df[~df[attA].str.match(regex_pat, na=False)]
    return df


#
# def doCond(cond, df):
#     # to do: given a condition, apply it and return the filtered df
#     return result_df
#
#
# def doOp(op, df1, df2):
#     # to do: given a op and two conditions, apply them and return the result df
#     return result_df
#
#
# def doWhere_new(query_list, df_list):
#     new_query_list = []
#     new_df_list = []
#     # do parenthesis first
#     for i in xrange(len(query_list)):
#         query = query_list[i]
#         df = df_list[i]
#         if not query.startwith("("):
#             new_query_list.append(query)
#             new_df_list.append(df)
#
#         if query.startwith("("):
#         # to do: find the right parentheis and remove both left and right and pass the trimmed query recursively
#             parenthesis_query_list = ["xxxx"]
#             parenthesis_df_list = ["xxxx"]
#             filtered_parenthesis_df = doWhere_new(parenthesis_query_list, parenthesis_df_list)
#             filtered_parenthesis_query = "filtered"
#             new_query_list.append(filtered_parenthesis_query)
#             new_df_list.append(filtered_parenthesis_df)
#
#     for i in xrange(len(new_query_list)):
#         query = query_list[i]
#         df = df_list[i]
#         # no parenthesis, do condition by sequence and return df
#         return result_df


