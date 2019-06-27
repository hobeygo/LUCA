import pandas as pd
from pandas import ExcelFile
import mysql.connector as mariadb

# DB connection
mariadb_connection = mariadb.connect(user='root', password='example', database='luca', port=3306)
cursor = mariadb_connection.cursor()

def insert_data(sheetname, tablename):
  df = pd.read_excel('ACCOUNTING_ENTRIES.xlsx', sheet_name=sheetname)
  for (index_label, row_series) in df.iterrows():
    column_count = df.shape[1]
    sql = "insert into "+tablename+" values ("
    for i in range(column_count):
      if i != 0:
        sql = sql+","
      # Check if int or not 
      if isinstance(row_series.values[i], int):
        sql = sql+str(row_series.values[i])
      else:
        sql = sql+"'"+str(row_series.values[i])+"'"
    sql = sql+")"
    print(sql)
    cursor.execute(sql)
    mariadb_connection.commit()

# Master BU PC
tablename = 'MASTER_BU_PC' 
try:
  try:
    cursor.execute("drop table "+tablename)
  except mariadb.Error as error:
    print("Error: {}".format(error))
  cursor.execute("create table "+tablename+"" \
                 "  (PCR_CODE      VARCHAR(10)," \
                 "   E_SPR_CODE_BU VARCHAR(10))" 
                 )
  insert_data("Master_BU_PC", tablename)
except mariadb.Error as error:
    print("Error: {}".format(error))

# Master_ACCOUNT_CODE
tablename = 'MASTER_ACCOUNT_CODE' 
try:
  try:
    cursor.execute("drop table "+tablename)
  except mariadb.Error as error:
    print("Error: {}".format(error))
  cursor.execute("create table "+tablename+"" \
                 "  (ACCOUNT        VARCHAR(10)," \
                 "   ACCOUNT_TYPE   VARCHAR(20)," \
                 "   DESCRIPTION    VARCHAR(100)," \
                 "   CLASSIFICATION VARCHAR(5))" 
                 )
  print("table "+tablename+" created")

  insert_data("MASTER_ACCOUNT_CODE", tablename)
except mariadb.Error as error:
    print("Error: {}".format(error))

