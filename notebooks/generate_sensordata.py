# Databricks notebook source
# MAGIC %pip install azure-eventhub
# MAGIC %pip install azure-storage
# MAGIC %pip install azure-servicebus==7.0.0
# MAGIC %pip install avro

# COMMAND ----------

import uuid
import datetime
import random
import json
from azure.servicebus import ServiceBusService
# Config this with your Azure EventHub parameters
az_service_namespace = 'iotsensordatacloubis'
az_shared_access_key_name = 'senderpolicy'
az_shared_access_key_value = 'vKpNWcHOKc8bFQRpSYY8wIQrAYxiZRdT++AEhLIIB7w='


sbs = ServiceBusService(service_namespace=az_service_namespace, shared_access_key_name=az_shared_access_key_name, shared_access_key_value=az_shared_access_key_value)
devices = []
for x in range(0, 10):
    devices.append(str(uuid.uuid4()))

for y in range(0,100000):
    for dev in devices:
        reading = {'source': 'python-code-caio-sensor2', 'id': dev, 'timestamp': str(datetime.datetime.utcnow()), 'uv': random.random(), 'temperature': random.randint(70, 100), 'humidity': random.randint(70, 100)}
        s = json.dumps(reading)
        sbs.send_event('eventhubiotsmarthouse', s)
    print (y)
    print (s)

# COMMAND ----------


