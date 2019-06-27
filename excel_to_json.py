import pandas as pd
from pandas import ExcelFile

df = pd.read_excel('CASA_entries_ex.xlsx', sheet_name='TABLE Structure')

#print("Column headings:")
#print(df.columns)
#print(df)
#df.to_json(r'CASA_entries_ex.json', orient='records')

for i in df.index:
    print(df.loc[i].to_json())
    # print(row.to_json())

#print(CASA_entries_ex.json)