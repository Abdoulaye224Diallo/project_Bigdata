import os
import json
import pandas as pd
from kafka import KafkaProducer

# Config Kafka Producer
KAFKA_TOPIC = "stationnement-data"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

DATA_FOLDER = "./producer/data"

for filename in os.listdir(DATA_FOLDER):
    if filename.endswith(".csv"):
        file_path = os.path.join(DATA_FOLDER, filename)
        try:
            # Tenter plusieurs encodages si besoin
            try:
                df = pd.read_csv(file_path, encoding='utf-8', on_bad_lines='skip')
            except UnicodeDecodeError:
                df = pd.read_csv(file_path, encoding='latin1', on_bad_lines='skip')
        except Exception as e:
            print(f"❌ Erreur lecture {filename} : {e}")
            continue
        
        print(f"📄 Envoi du fichier {filename} avec {len(df)} lignes...")

        for i, row in df.iterrows():
            record = json.loads(row.to_json())
            record["source_file"] = filename
            record["row_number"] = i
            producer.send(KAFKA_TOPIC, value=record)

        print(f"✅ {filename} envoyé avec succès.")

producer.flush()
producer.close()

print("✅ Tous les fichiers ont été envoyés à Kafka.")
