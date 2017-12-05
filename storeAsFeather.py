from pandas import *
from os.path import *
import glob 

#store = HDFStore('store.h5')

data = {}
for fileName in glob.glob(('*.csv')):
    df = read_csv(fileName, parse_dates=True, infer_datetime_format=True)
    file = fileName.split('.')
    table_name = file[0].lstrip()
    # stringColumn = []
    # for column in df.columns:
    	# if df[column].dtype.kind=='O':
	# 		stringColumn.append(column)
	# df[stringColumn]=df[stringColumn].fillna(value='')
    #store[table_name] = df
    # data[table_name]=df

    df.to_feather(table_name+".feather")
#print store