import io
import avro.schema
import avro.io
import json
from kafka import KafkaConsumer

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
    print(acc_event["PAR_PROCESS_DATE"])