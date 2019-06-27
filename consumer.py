import io
import avro.schema
import avro.io
import json
import mysql.connector as mariadb
from avro.io import DatumWriter
from kafka import KafkaConsumer
from kafka import SimpleProducer
from kafka import KafkaClient
#open mariadb cursor

mariadb_connection = mariadb.connect(user='root', password='example', database='luca', port=3306)
cursor = mariadb_connection.cursor(buffered=True)



# To consume messages
CONSUMER = KafkaConsumer('CASA_events',
                         group_id='my_group',
                         bootstrap_servers=['kafka-1:19092'])

# To send messages synchronously
KAFKA = KafkaClient('kafka-1:19092')
PRODUCER = SimpleProducer(KAFKA)

# Path to CASA Events avro schema
SCHEMA_PATH = "CASA_events.avsc"
SCHEMA = avro.schema.Parse(open(SCHEMA_PATH).read())

# Path to CASA Entries avro schema
SCHEMA_PATH_OUTPUT = "CASA_entries.avsc"
SCHEMA_OUTPUT = avro.schema.Parse(open(SCHEMA_PATH_OUTPUT).read())

i = 1
for msg in CONSUMER:
    print("Process CASA Event " + str(i))
    bytes_reader = io.BytesIO(msg.value)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(SCHEMA)
    acc_event = reader.read(decoder)  
    #print(acc_event)
    #loaded_json = json.loads(acc_event)
    #Getting a particular field out of the row event data
    # print(acc_event["EVENT_CODE"])
    # print(acc_event["BANK_ACCOUNT_TYPE"])
    # param1=acc_event["EVENT_CODE"]
    # param2=acc_event["BANK_ACCOUNT_TYPE"]
    # cursor.execute("""select account_code, dc from rule_output where rule_id in (SELECT rule_id from rule_list where event_code = 5500 and bank_account_type = "LOROA")""")
    cursor.execute("select rule_id, account_code, dc from rule_output where rule_id in (SELECT rule_id from rule_list where event_code = %s and bank_account_type = %s)",(acc_event["EVENT_CODE"], acc_event["BANK_ACCOUNT_TYPE"]))
    for rule_id, account_code, dc in cursor:
        print("Match rule " + str(rule_id))
        acc_entry = acc_event
        acc_entry["ACT_CODE"] = account_code
        acc_entry["DC_FLAG"] = dc

        writer = DatumWriter(SCHEMA_OUTPUT)
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer.write(acc_entry,encoder)

        # writer.write(json.dumps(acc_entry),encoder)
        raw_bytes = bytes_writer.getvalue()
        PRODUCER.send_messages("CASA_entries", raw_bytes)
    
    i = i + 1
