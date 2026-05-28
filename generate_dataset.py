"""
Run this to write live Kafka sensor data into InfluxDB for Grafana dashboards.
Keep it running alongside the simulator.
"""
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import json, random, time

client = InfluxDBClient(url='http://localhost:8086', token='my-super-secret-token', org='iot-org')
write_api = client.write_api(write_options=SYNCHRONOUS)

consumer = KafkaConsumer(
    'sensor-temperature', 'sensor-vibration', 'sensor-pressure',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id='influx-writer',
    auto_offset_reset='latest'
)

print('Writing sensor data to InfluxDB... (Ctrl+C to stop)')
count = 0
for msg in consumer:
    e = msg.value
    point = Point('sensor_readings') \
        .tag('machine_id', e['machine_id']) \
        .tag('sensor', e['sensor']) \
        .field('value', float(e['value'])) \
        .field('state', int(e['state']))
    write_api.write(bucket='sensor-data', record=point)
    count += 1
    if count % 100 == 0:
        print(f'Written {count} points to InfluxDB...')
