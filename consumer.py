import io
import avro.schema
import avro.io
import json
import mysql.connector as mariadb
from kafka import KafkaConsumer

#open mariadb cursor

mariadb_connection = mariadb.connect(user='root', password='example', database='luca', port=3306)
cursor = mariadb_connection.cursor()



# To consume messages
CONSUMER = KafkaConsumer('CASA_events',
                         group_id='my_group',
                         bootstrap_servers=['kafka-1:19092'])

SCHEMA_PATH = "CASA_events.avsc"
SCHEMA = avro.schema.Parse(open(SCHEMA_PATH).read())

for msg in CONSUMER:
    bytes_reader = io.BytesIO(msg.value)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(SCHEMA)
    acc_event = reader.read(decoder)  
    #print(acc_event)
    #loaded_json = json.loads(acc_event)
    #Getting a particular field out of the row event data
    print(acc_event["EVENT_CODE"])
    print(acc_event["BANK_ACCOUNT_TYPE"])
    cursor.execute("select Account_code, D/C from rule_output where rule_id in (SELECT rule_id from rule_list where event_code = %s and bank_account_type = %t"),((param1, param2)))
