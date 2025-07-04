from kafka import KafkaConsumer
import json
from minio import Minio
from io import BytesIO

# ✅ Kafka Consumer
consumer = KafkaConsumer(
    'sncf-data',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# ✅ MinIO client (port API S3)
minio_client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

bucket_name = "sncf-bucket"
if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)
    print(f"✅ Bucket '{bucket_name}' créé dans MinIO.")
else:
    print(f"✅ Bucket '{bucket_name}' déjà présent dans MinIO.")

# ✅ Lecture depuis Kafka et upload dans MinIO
for message in consumer:
    print("📥 Message reçu de Kafka :", message.value)
    data = json.dumps(message.value).encode('utf-8')
    file_name = f"sncf_{message.offset}.json"
    minio_client.put_object(
        bucket_name,
        file_name,
        data=BytesIO(data),
        length=len(data),
        content_type='application/json'
    )
    print(f"✅ {file_name} uploadé dans MinIO.")


