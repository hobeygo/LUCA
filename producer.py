import io
import random
import avro
from avro.io import DatumWriter
from kafka import SimpleProducer
from kafka import KafkaClient

# To send messages synchronously
KAFKA = KafkaClient('kafka-1:19092')
PRODUCER = SimpleProducer(KAFKA)

# Kafka topic
TOPIC = "CASA_events"

# Path to user.avsc avro schema
SCHEMA_PATH = "CASA_events.avsc"
SCHEMA = avro.schema.Parse(open(SCHEMA_PATH).read())

for i in range(10):
    writer = DatumWriter(SCHEMA)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write({"name": "123", "favorite_color": "111", "favorite_number": random.randint(0, 10)}, encoder)
    raw_bytes = bytes_writer.getvalue()
    PRODUCER.send_messages(TOPIC, raw_bytes)