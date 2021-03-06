import shlex
from pandas import *
from os.path import *
from copy import *
from interpreter import *
import re

# @parameter: df- final dataframe after querying from WHERE clause
#             attrs - attributes we are going to project
# @return: the dataframe after projection
def doSELECT(df, attrs,distinct):
    columns = []
    if attrs[0] == '*':
        return df
    else:
        for attr in attrs:
            attr = attr.split('.')
            if len(attr) == 2:
                columns.append(attr[1])
                #columns.append(attr[1].split('00')[1])
                #df.rename(columns={attr[1]: attr[1].split('00')[1]}, inplace=True)
            else:
                columns.append(attr[0])
                # columns.append(attr[0].split('00')[1])
                # df.rename(columns={attr[0]: attr[0].split('00')[1]}, inplace=True)
    if distinct:
        df = df[columns].drop_duplicates()
    else:
        df = df[columns]
    return df


# WHERE clause does the cartesian product of all relations
def doWHERE(query, panel):
    temp_panel = copy(panel)
    final_df = panel.itervalues().next()
    preOP = None
    notOP = None
    for i in range(len(query)):
        #cond = shlex.split(query[i])
        if i>= len(query):
            break
        cond = re.findall('"[^"]*"|\'[^\']*\'|[^"\'\s]+', query[i])
        if cond[0] == '(':
            subquery = query[i+1:query[::-1].index(')')-1]
            sub_query_result = doWHERE(subquery, panel)
            if preOP == 'AND':
                final_df = merge(final_df, sub_query_result)
            elif preOP == 'OR':
                final_df = concat([final_df, sub_query_result]).drop_duplicates()
            else:
                final_df = sub_query_result
            if query[::-1].index(')') < len(query) - 1:
                query = query[0:i] + query[query[::-1].index(')')+1:]
                continue
            else:
                print "illegal"
                return final_df
        elif cond[0] == ')':
            continue
        elif cond[0] in ('AND', 'OR'):
            preOP = cond[0]
            continue
        elif cond[0] == 'NOT':
            notOP = cond[0]
            continue
        else:
            tableA, attrA = None,None
            tableB, attrB = None, None
            op = ''
            stm = ''

            for token in cond:
                a = token.split('.')
                if len(a)==2 and tableA==None and attrA==None:
                    tableA, attrA = a
                    stm+=attrA
                elif len(a)==2 and tableB==None and attrB==None:
                    tableB, attrB = a
                    stm+=attrB
                else:
                    if token in ('==','<>','>=','<=','<','>','LIKE'):
                        op=token
                    stm+=token
            if preOP == 'AND':
                listA = [key for key in temp_panel if tableA in key]
                listB = [key for key in temp_panel if tableB is not None and tableB in key]
                if listA:
                    tableA = listA[0]
                    dfA = temp_panel[tableA]
                if listB:
                    tableB = listB[0]
                    dfB = temp_panel[tableB]
            else: # preOP == 'OR'
                dfA = panel[tableA]
                if tableB is not None:
                    dfB = panel[tableB]
            if len(cond)==1:
                stm = attrA if notOP != 'NOT' else attrA + '== False'
                df = dfA.query(stm)
            elif op == 'LIKE':
                df = doLIKE(dfA, cond[2], attrA, notOP)
            elif op != 'LIKE':
                if tableA==tableB or tableB==None:
                    if notOP!='NOT':
                        df = dfA.query(stm)
                    else:
                        df = dfA.query('not '+stm)
                elif tableA!=tableB and tableB!=None:
                    if (op == '==' and notOP is None) or (op == '<>' and notOP == 'NOT'):
                        df = merge(dfB, dfA, left_on=attrB, right_on=attrA)
                    else:
                        dfB['key'] = 0
                        dfA['key'] = 0
                        df = merge(dfB, dfA, on='key')
                        df = df.query(stm) if notOP != 'NOT' else df.query('not '+stm)
            if preOP == 'OR':
                if tableB != None: #Very unlikely to be executed
                    df = concat([temp_panel[tableB + tableA], df]).drop_duplicates()
                    temp_panel[tableA + tableB] = df
                    temp_panel.pop(tableA,None)
                    temp_panel.pop(tableB,None)
                else:
                    # temp_panel[tableA]=merge(temp_panel[tableA].reset_index(),df.reset_index(),how='outer').set_index(attrA)
                    #print temp_panel[tableA]
                    #print df
                    df = concat([temp_panel[tableA], df]).drop_duplicates()
                    temp_panel[tableA] = df
            else:
                if tableB != None:
                    temp_panel[tableA + tableB] = df
                    temp_panel.pop(tableA, None)
                    temp_panel.pop(tableB, None)
                else:
                    temp_panel[tableA] = df
            notOP = None
            final_df = df

            # tableA, attrA = cond[0].split('.')
            # tableB, attrB, valueB = None, None, None
            # op = None
            # if len(cond)==3:
            #     op = cond[1]
            #     tempB = cond[2].split('.') # A <op> B where B might be value or table.attribute
            #     if len(tempB) == 2:
            #         tableB, attrB = tempB
            #     else:
            #         valueB = tempB[0]
            # if preOP == 'AND':
            #     listA = [key for key in temp_panel if tableA in key]
            #     listB = [key for key in temp_panel if tableB is not None and tableB in key]
            #     if listA:
            #         tableA = listA[0]
            #         dfA = temp_panel[tableA]
            #     if listB:
            #         tableB = listB[0]
            #         dfB = temp_panel[tableB]
            # else: # preOP == 'OR'
            #     dfA = panel[tableA]
            #     if tableB is not None:
            #         dfB = panel[tableB]
            # if len(cond)==1:
            #     stm = attrA if notOP != 'NOT' else attrA + '== False'
            #     df = dfA.query(stm)
            # elif op == 'LIKE':
            #     df = doLIKE(dfA, tempB[0], attrA, notOP)
            # elif op != 'LIKE': # A <op> B where <op> is not 'LIKE'
            #     # If A is attribute and B is value
            #     if valueB != None:
            #         #print valueB
            #         stm = attrA + op + valueB if notOP != 'NOT' else 'not ' + attrA + op + valueB
            #         #print dfA
            #         df = dfA.query(stm)
            #     # If A and B are all attribute and at the same table
            #     elif tableA == tableB:
            #         stm = attrA + op + attrB if notOP != 'NOT' else 'not ' + attrA + op + attrB
            #         df = dfA.query(stm)
            #     # If A and B are all attribute and at different tables
            #     elif tableA != tableB:
            #         if (op == '==' and notOP is None) or (op == '<>' and notOP == 'NOT'):
            #             df = merge(dfB, dfA, left_on=attrB, right_on=attrA)
            #         else:
            #             dfB['key'] = 0
            #             dfA['key'] = 0
            #             df = merge(dfB, dfA, on='key')
            #             stm = attrA + op + attrB if notOP != 'NOT' else 'not ' + attrA + op + attrB
            #             df = df.query(stm)
            # if preOP == 'OR':
            #     if tableB != None: #Very unlikely to be executed
            #         df = concat([temp_panel[tableB + tableA], df]).drop_duplicates()
            #         temp_panel[tableA + tableB] = df
            #         temp_panel.pop(tableA,None)
            #         temp_panel.pop(tableB,None)
            #     else:
            #         # temp_panel[tableA]=merge(temp_panel[tableA].reset_index(),df.reset_index(),how='outer').set_index(attrA)
            #         #print temp_panel[tableA]
            #         #print df
            #         df = concat([temp_panel[tableA], df]).drop_duplicates()
            #         temp_panel[tableA] = df
            # else:
            #     if tableB != None:
            #         temp_panel[tableA + tableB] = df
            #         temp_panel.pop(tableA, None)
            #         temp_panel.pop(tableB, None)
            #     else:
            #         temp_panel[tableA] = df

            # notOP = None
            # final_df = df
    return final_df



def doLIKE(df, b, attA, noop):
    #df.reset_index(level=[attA], inplace=True)
    b = shlex.split(b)[0]
    regex_pat = b.replace('%', '.*')
    regex_pat = regex_pat.replace('_', '.')
    if noop <> 'NOT':
        df = df[df[attA].str.match(regex_pat, na=False)]
    else:
        df = df[~df[attA].str.match(regex_pat, na=False)]
    return df



