import random
import time

import requests
import uuid

device_uuid = uuid.uuid1()
url = 'http://metricsapi:8080/devices/{}/readings'.format(device_uuid)
sensors = ['temperature', 'humidity', 'pressure', 'light', 'smoke']
while True:
    data = {
        "type": random.choice(sensors),
        "value": random.randint(0, 100),
        "date_created": int(time.time())
    }
    requests.post(url, data=data)
