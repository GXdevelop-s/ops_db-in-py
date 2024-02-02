import datetime
import json
import sys
import time
import random

import orjson
import pyzstd
from confluent_kafka import Producer

kafka_config = {

    "bootstrap.servers": '127.0.0.1:9092'
}

producer = Producer(kafka_config)

start_time = (datetime.datetime.strptime("2023-08-09 17:33:00", "%Y-%m-%d %H:%M:%S").timestamp() * 1000)

result = []
with pyzstd.open('test.log', mode='rb') as f:
    for line in f:
        result.append(json.loads(line))

index = len(result)
delta_timestamp = start_time - result[0]['_ts']
log_length = len(result)
end_timestamp = result[0]['_ts']
while True:
    if index >= log_length:
        current_timestamp = end_timestamp + 1000
        log = random.choice(result)
        log['_ts'] = current_timestamp
        if current_timestamp >= time.time() * 1000:
            log['_ts'] = time.time() * 1000
            time.sleep(1)
    else:
        log = result[index]
        log['_ts'] = log['_ts'] + delta_timestamp
    producer.produce(sys.argv[1], value=orjson.dumps(log))
    if index % 100 == 0:
        producer.flush()
    index += 1
    end_timestamp = log['_ts']
    print(datetime.datetime.fromtimestamp(log['_ts'] / 1000).strftime('%Y-%m-%d %H:%M:%S'))
