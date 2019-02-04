"""
python3 streamProducer.py 100
"""
from kafka import KafkaProducer
import json
import sys
from time import sleep
import os

dataset = set()
while "train.csv" not in dataset:
	if os.path.exists("train.csv"):
		if os.path.isfile("train.csv"):
			dataset.add("train.csv")   
	if "train.csv" not in dataset:
		print("could'nt find train.csv in $SPARK_HOME, waiting...")
	sleep(5)
	
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], acks=1, linger_ms=10, retries=5)	
with open("train.csv", "r") as fp:
	count=0
	for line in fp:
		print(line)
		record = line.replace("\n","")
		producer.send("spark",bytes(record,encoding="utf-8"))
		count=count +1
		sleep(0.5)

producer.flush()
producer.close()


