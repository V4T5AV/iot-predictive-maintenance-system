from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime

INFLUX_URL    = 'http://localhost:8086'
INFLUX_TOKEN  = 'my-super-secret-token'
INFLUX_ORG    = 'iot-org'
INFLUX_BUCKET = 'sensor-data'

client    = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = client.write_api(write_options=SYNCHRONOUS)

def write_sensor_reading(machine_id, sensor, value):
    point = Point('sensor_readings') \
        .tag('machine_id', machine_id) \
        .tag('sensor', sensor) \
        .field('value', float(value))
    write_api.write(bucket=INFLUX_BUCKET, record=point)

def write_failure_probability(machine_id, probability, is_alert):
    point = Point('failure_probability') \
        .tag('machine_id', machine_id) \
        .field('probability', float(probability)) \
        .field('is_alert', int(is_alert))
    write_api.write(bucket=INFLUX_BUCKET, record=point)
