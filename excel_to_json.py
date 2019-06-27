import pandas as pd
from pandas import ExcelFile

df = pd.read_excel('example.xlsx', sheet_name='Sheet1')

print("Column headings:")
print(df.columns)
print(df)
df.to_json(r'example.json', orient='records')