import json
import time

from kafka import KafkaProducer
from sklearn.model_selection import train_test_split
import random
import pandas as pd
producer = KafkaProducer(bootstrap_servers='localhost:9092', security_protocol="PLAINTEXT", api_version=(3, 5, 0))

data = pd.read_csv("diabetes_binary_health_indicators_BRFSS2015.csv", delimiter=",", header=0)

labels = data["Diabetes_binary"]
data = data.drop("Diabetes_binary", axis=1)
offline, onlineData, offlineLabels, onlineLabels = train_test_split(data, labels, test_size=0.2, random_state=42)

for data in onlineData.to_dict(orient="records"):
    print(json.dumps(data))
    producer.send(
        topic="health_data",
        value=json.dumps(data).encode("utf-8")
    )
    time.sleep(random.randint(500, 2000) / 1000.0)
