from kafka import KafkaConsumer
from minio import Minio
from io import BytesIO
import json

# Config Kafka Consumer
consumer = KafkaConsumer(
    'stationnement-data',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Connexion MinIO (port 9000 par défaut)
minio_client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

bucket_name = "stationnement-bucket"

# Créer le bucket s'il n'existe pas
if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)
    print(f"✅ Bucket '{bucket_name}' créé.")
else:
    print(f"✅ Bucket '{bucket_name}' existe déjà.")

print("⏳ En attente des messages Kafka...")

# Consommation et upload dans MinIO
for message in consumer:
    data = message.value
    # On convertit le message en JSON binaire
    json_bytes = json.dumps(data).encode('utf-8')
    
    # Nom du fichier unique par offset kafka (tu peux adapter)
    file_name = f"stationnement_{message.offset}.json"
    
    # Upload dans MinIO
    minio_client.put_object(
        bucket_name,
        file_name,
        data=BytesIO(json_bytes),
        length=len(json_bytes),
        content_type="application/json"
    )
    
    print(f"✅ Message offset {message.offset} stocké dans MinIO sous '{file_name}'.")
