from pandas import *
from os.path import *
import glob 

store = HDFStore('store.h5')

for fileName in glob.glob(('*.csv')):
    df = read_csv(fileName, parse_dates=True, infer_datetime_format=True)
    file = fileName.split('.')
    table_name = file[0].lstrip()
    store[table_name] = df
    print table_name

print store