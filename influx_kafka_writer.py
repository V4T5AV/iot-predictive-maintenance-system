from kafka import KafkaConsumer
from collections import defaultdict, deque
import json, requests, statistics

KAFKA_BROKER = 'localhost:9092'
SCORING_URL  = 'http://localhost:5001/predict'

windows = defaultdict(lambda: defaultdict(lambda: deque(maxlen=60)))

consumer = KafkaConsumer(
    'sensor-temperature', 'sensor-vibration', 'sensor-pressure',
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id='alert-consumer-group',
    auto_offset_reset='latest'
)

def compute_window_stats(machine_id):
    w = windows[machine_id]
    def safe_mean(lst): return sum(lst)/len(lst) if lst else 0
    def safe_max(lst):  return max(lst) if lst else 0
    def safe_std(lst):  return statistics.stdev(lst) if len(lst) > 1 else 0
    temps = list(w['temperature'])
    vibs  = list(w['vibration'])
    press = list(w['pressure'])
    return {
        'machine_id':    machine_id,
        'avg_temp':      round(safe_mean(temps), 2),
        'max_temp':      round(safe_max(temps),  2),
        'avg_vibration': round(safe_mean(vibs),  3),
        'max_vibration': round(safe_max(vibs),   3),
        'avg_pressure':  round(safe_mean(press), 2),
        'pressure_std':  round(safe_std(press),  2),
        'temp_trend':    round((temps[-1]-temps[0])/max(len(temps),1), 3) if temps else 0,
        'spike_count':   sum(1 for v in vibs if v > 0.55),
    }

score_counter = 0
print('Alert consumer started. Listening for sensor data...')
for msg in consumer:
    event  = msg.value
    mid    = event['machine_id']
    sensor = event['sensor']
    windows[mid][sensor].append(event['value'])
    score_counter += 1
    if score_counter % 100 == 0:
        for machine_id in list(windows.keys()):
            stats = compute_window_stats(machine_id)
            try:
                r      = requests.post(SCORING_URL, json=stats, timeout=5)
                result = r.json()
                prob   = result['failure_prob']
                status = 'ALERT' if result['alert'] else 'OK'
                print(f'[{status}] {machine_id} | Failure prob: {prob:.1%}')
                if result['alert']:
                    print(f'  *** HIGH RISK: {machine_id} requires inspection ***')
            except Exception as e:
                print(f'Scoring error: {e}')
