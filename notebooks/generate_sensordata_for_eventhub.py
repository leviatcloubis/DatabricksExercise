# Databricks notebook source
# MAGIC %pip install azure-eventhub
# MAGIC %pip install azure-storage
# MAGIC #%pip install avro
# MAGIC %pip install azure-servicebus==0.21.1

# COMMAND ----------

import uuid
import datetime
import random
import json
from azure.servicebus import ServiceBusService
# Config this with your Azure EventHub parameters
az_service_namespace = 'iotaprilsensordata'
az_shared_access_key_name = 'Senderpolicy'
az_shared_access_key_value = '+u8+DgOQ109bT7cVTq8ujdnjMLBgIXa/f+AEhEErMfQ='


sbs = ServiceBusService(service_namespace=az_service_namespace, shared_access_key_name=az_shared_access_key_name, shared_access_key_value=az_shared_access_key_value)
creators = ['El_Patron_Rostenberghe','El_Patron_Devos']
validators = ['Cloubis','Infofarm']
devices = []

for x in range(0, 10):
    devices.append(str(uuid.uuid4()))

for y in range(0,100000):
    for dev in devices:
        reading = {'source': 'python-code-caio-sensor2', 'id': dev, 'timestamp': str(datetime.datetime.utcnow()), 'uv': random.random(), 'temperature': random.randint(70, 100), 'humidity': random.randint(70, 100), 'CreatedBy': random.choice(creators), 'Validator': random.choice(validators)}
        #csvv = f"source,CreatedBy,timestamp\python-code-caio-sensor2,{random.choice(creators)},{str(datetime.datetime.utcnow())}"
        s = json.dumps(reading)
        sbs.send_event('Iotaprilsensoreventhub', s)
    print (y)
    print (s)

# COMMAND ----------


