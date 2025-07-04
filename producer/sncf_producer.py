from kafka import KafkaProducer
import pandas as pd
import json
 
# Lecture du parquet
df = pd.read_parquet("./producer/data_sncf/ponctualite-mensuelle-transilien.parquet")
 
# Init du producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
 
# Compteur pour l'ID auto-incrémenté
id_counter = 1
 
# Envoi ligne par ligne
for _, row in df.iterrows():
    record = json.loads(row.to_json())
    record["id"] = id_counter
    id_counter += 1
    producer.send('sncf-data', value=record)
 
producer.flush()
producer.close()
 
print("✅ Données envoyées à Kafka avec ID auto-incrémenté.")
 


 