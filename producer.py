import io
import random
import avro
from avro.io import DatumWriter
from kafka import SimpleProducer
from kafka import KafkaClient
import pandas as pd
from pandas import ExcelFile
import json

# To send messages synchronously
KAFKA = KafkaClient('kafka-1:19092')
PRODUCER = SimpleProducer(KAFKA)

# Kafka topic
TOPIC = "CASA_events"

# Path to user.avsc avro schema
SCHEMA_PATH = "CASA_events.avsc"
SCHEMA = avro.schema.Parse(open(SCHEMA_PATH).read())

#read excel file
df = pd.read_excel('CASA_entries_ex.xlsx', sheet_name='TABLE Structure')
print(df.loc[0].to_json())


#produce to the kafka topic using the df.loc[i].to_json()
for i in df.index:
   writer = DatumWriter(SCHEMA)
   bytes_writer = io.BytesIO()
   encoder = avro.io.BinaryEncoder(bytes_writer)
   writer.write(json.loads(df.loc[i].to_json()),encoder)
   # writer.write({"PAR_PROCESS_DATE":"23-06-19","PAR_SOURCE":"PAY","AMOUNT":133.0,"AMOUNT_CCY":"BGN","BCE_AMOUNT":133.0,"BCE_AMOUNT_CCY":"BGN","CONTRACT_ID":"BG05INGB91451400018310","EVENT_CODE":5506,"E_ATE_CODE":"CHARGES RECD","E_PDT_CODE":"PAYMT","E_RLN_PGP_COUNTRY_ISO_CODE":"BG","E_RLN_PGP_ZONE_SECTOR":"PRIV SECTOR","PCR_CODE":7503,"PC_MONTH":201906,"ALT_CONTRACT_REF":"PAY\/BG05INGB91451400018310","E_SPR_CODE":"U2989_01","ACCOUNT_CLASSIFICATION":"P&L","E_FDK_IMT_NAME":"NON-TRADING","E_FDK_MT":"AMC","E_RCL_IND":"N","E_FV_OPTION":"N","E_ACT_STD":"IFRS9","BANK_ACCOUNT_TYPE":"CACOR"},encoder)
   raw_bytes = bytes_writer.getvalue()
   PRODUCER.send_messages(TOPIC, raw_bytes)
  


""" for i in df:
   writer = DatumWriter(SCHEMA)
   bytes_writer = io.BytesIO()
   encoder = avro.io.BinaryEncoder(bytes_writer)
   #writer.write({"
   PAR_PROCESS_DATE": "23-6-2019", "PAR_SOURCE": "PAY", "AMOUNT": 133, "AMOUNT_CCY": "BGN", "BCE_AMOUNT": 133, "BCE_AMOUNT_CCY": "BGN", "CONTRACT_ID": "BG05INGB91451400018310", "EVENT_CODE": "5506", "E_ATE_CODE": "CHARGES RECD", "E_PDY_CODE": "PAYMT", "E_RLN_PGP_COUNTRY_ISO_CODE": "BG", "E_RLN_PGP_ZONE_SECTOR": "PRIV SECTOR", "PCR_CODE": "7503", "PC_MONTH": "201906", "ALT_CONTRACT_REF": "PAY/BG05INGB91451400018310", "E_SPR_CODE": "U2989_01", "ACCOUNT_CLASSIFICATION": "P&L", "E_FDK_IMT_NAME": "NON-TRADING", "E_FDK_MT": "AMC", "E_RCL_IND": "N", "E_FV_OPTION": "N", "E_ACT_STD": "IFRS9", "BANK_ACCOUNT_TYPE": "CACOR"}, encoder)

   "PAR_PROCESS_DATE":"23-06-19","PAR_SOURCE":"PAY","AMOUNT":133.0,"AMOUNT_CCY":"BGN","BCE_AMOUNT":133.0,"BCE_AMOUNT_CCY":"BGN","CONTRACT_ID":"BG05INGB91451400018310","EVENT_CODE":5506,"E_ATE_CODE":"CHARGES RECD","E_PDT_CODE":"PAYMT","E_RLN_PGP_COUNTRY_ISO_CODE":"BG","E_RLN_PGP_ZONE_SECTOR":"PRIV SECTOR","PCR_CODE":7503,"PC_MONTH":201906,"ALT_CONTRACT_REF":"PAY\/BG05INGB91451400018310","E_SPR_CODE":"U2989_01","ACCOUNT_CLASSIFICATION":"P&L","E_FDK_IMT_NAME":"NON-TRADING","E_FDK_MT":"AMC","E_RCL_IND":"N","E_FV_OPTION":"N","E_ACT_STD":"IFRS9","BANK_ACCOUNT_TYPE":"CACOR"
   writer.write(i)
   raw_bytes = bytes_writer.getvalue()
   PRODUCER.send_messages(TOPIC, raw_bytes)
 """