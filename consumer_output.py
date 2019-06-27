import io
import avro.schema
import avro.io
import json
import mysql.connector as mariadb
from kafka import KafkaConsumer



# To consume messages
CONSUMER = KafkaConsumer('CASA_entries',
                         group_id='my_group',
                         bootstrap_servers=['kafka-1:19092'])

# Path to CASA Entries avro schema
SCHEMA_PATH = "CASA_entries.avsc"
SCHEMA = avro.schema.Parse(open(SCHEMA_PATH).read())

i = 1
for msg in CONSUMER:
    bytes_reader = io.BytesIO(msg.value)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(SCHEMA)
    acc_entry = reader.read(decoder)
    print("Record CASA Entry " + str(i))
    print(acc_entry)
    i = i + 1

    
