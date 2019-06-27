import pandas as pd
from pandas import ExcelFile
import mysql.connector as mariadb

# DB connection
mariadb_connection = mariadb.connect(user='root', password='example', database='luca', port=3306)
cursor = mariadb_connection.cursor()

def insert_data(excelsheet, sheetname, tablename):
  df = pd.read_excel(excelsheet, sheet_name=sheetname)
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
  insert_data('ACCOUNTING_ENTRIES.xlsx', "Master_BU_PC", tablename)
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

  insert_data('ACCOUNTING_ENTRIES.xlsx', "MASTER_ACCOUNT_CODE", tablename)
except mariadb.Error as error:
    print("Error: {}".format(error))

# EVENTS
tablename = 'EVENTS' 
try:
  try:
    cursor.execute("drop table "+tablename)
  except mariadb.Error as error:
    print("Error: {}".format(error))
  cursor.execute("create table "+tablename+"" \
                 "  (EVENT_CODE      VARCHAR(10)," \
                 "   EVENT_NAME      VARCHAR(100)," \
                 "   EVENT_CATEGORY  VARCHAR(100))" 
                 )
  print("table "+tablename+" created")

  insert_data("EVENT_SCHEMA.xlsx", "EVENTS", tablename)
except mariadb.Error as error:
    print("Error: {}".format(error))

# EVENTS
tablename = 'SCHEME' 
try:
  try:
    cursor.execute("drop table "+tablename)
  except mariadb.Error as error:
    print("Error: {}".format(error))
  cursor.execute("create table "+tablename+"" \
                 "  (SCHEME_CODE_ACCOUNT_TYPE VARCHAR(10)," \
                 "   CUSTOMER_ACCOUNT         VARCHAR(500))" 
                 )
  print("table "+tablename+" created")

  insert_data("EVENT_SCHEMA.xlsx", "SCHEMA", tablename)
except mariadb.Error as error:
    print("Error: {}".format(error))

# FINAL INCOMING DATA
tablename = 'FINAL_INCOMING_DATA' 
try:
  try:
    cursor.execute("drop table "+tablename)
  except mariadb.Error as error:
    print("Error: {}".format(error))
  cursor.execute("create table "+tablename+"" \
                "  (PAR_PROCESS_DATE            DATE," \
                "   PAR_SOURCE                  VARCHAR(20)," \
                "   AMOUNT                      VARCHAR(100)," \
                "   AMOUNT_CCY                  VARCHAR(5)," \
                "   BCE_AMOUNT_CCY              VARCHAR(50)," \
                "   BCE_AMOUNT                  VARCHAR(10)," \
                "   CONTRACT_ID                 VARCHAR(50)," \
                "   EVENT_CODE                  VARCHAR(100)," \
                "   E_ATE_CODE                  VARCHAR(50)," \
                "   E_PDT_CODE                  VARCHAR(10)," \
                "   E_RLN_PGP_COUNTRY_ISO_CODE  VARCHAR(20)," \
                "   E_RLN_PGP_ZONE_SECTOR       VARCHAR(100)," \
                "   PCR_CODE                    VARCHAR(5)," \
                "   PC_MONTH                    VARCHAR(10)," \
                "   ALT_CONTRACT_REF            VARCHAR(50)," \
                "   E_SPR_CODE                  VARCHAR(10)," \
                "   ACCOUNT_CLASSIFICATION      VARCHAR(20)," \
                "   E_FDK_IMT_NAME              VARCHAR(100)," \
                "   E_FDK_MT                    VARCHAR(5)," \
                "   E_RCL_IND                   VARCHAR(10)," \
                "   E_FV_OPTION                 VARCHAR(20)," \
                "   E_ACT_STD                   VARCHAR(100)," \
                "   BANK_ACCOUNT_TYPE           VARCHAR(5)," \
                "   VALUE_DATE                  DATE)"
                )
  print("table "+tablename+" created")

  insert_data("ACCOUNTING_ENTRIES.xlsx", "FINAL_INCOMING_DATA", tablename)
except mariadb.Error as error:
    print("Error: {}".format(error))

