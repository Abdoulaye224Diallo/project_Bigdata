from minio import Minio
import snowflake.connector
import json
from datetime import datetime

# Connexion MinIO
minio_client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

bucket_name = "sncf-bucket"

# Connexion Snowflake
conn = snowflake.connector.connect(
    user='DIALLO224',
    password='Efreiecole224@',
    account='uadrunu-tc91125',
    warehouse='COMPUTE_WH',
    database='DATA_LAKE',
    schema='PUBLIC'
)

cursor = conn.cursor()

# Création de la table SNCF_DATA si elle n'existe pas
create_table_sql = """
CREATE TABLE IF NOT EXISTS SNCF_DATA (
    id INTEGER PRIMARY KEY,
    date VARCHAR,
    service VARCHAR,
    ligne VARCHAR,
    nom_de_la_ligne VARCHAR,
    taux_de_ponctualite FLOAT,
    nombre_de_voyageurs_a_l_heure_pour_un_voyageur_en_retard FLOAT
)
"""
cursor.execute(create_table_sql)
print("✅ Table SNCF_DATA vérifiée/créée.")

# Fonction pour nettoyer et convertir les valeurs
def clean_value(val, col_name=None):
    if val is None:
        return None
    if col_name == 'date':
        try:
            dt = datetime.utcfromtimestamp(val / 1000)
            return dt.strftime('%d/%m/%Y')
        except Exception as e:
            print(f"⚠️ Erreur conversion date: {e}")
            return None
    if isinstance(val, str):
        v = val.strip()
        if v == "" or v.lower() == "n/a":
            return None
        return v
    return val

objects = minio_client.list_objects(bucket_name)

for obj in objects:
    print(f"Traitement de l'objet: {obj.object_name}")
    try:
        response = minio_client.get_object(bucket_name, obj.object_name)
        data_bytes = response.read()
        response.close()
        response.release_conn()

        data = json.loads(data_bytes)

        if 'id' not in data:
            print(f"⚠️ Objet {obj.object_name} ignoré car pas de clé 'id'.")
            continue

        data_clean = {k: clean_value(v, k) for k, v in data.items()}

        columns = ', '.join(data_clean.keys())
        values_placeholders = ', '.join(['%s'] * len(data_clean))
        values = tuple(data_clean.values())

        merge_sql = f"""
        MERGE INTO SNCF_DATA t
        USING (SELECT %s AS id) s
        ON t.id = s.id
        WHEN NOT MATCHED THEN
          INSERT ({columns})
          VALUES ({values_placeholders})
        """

        params = (data_clean['id'],) + values

        cursor.execute(merge_sql, params)
        print(f"✅ Objet {obj.object_name} inséré/merge avec succès.")

    except Exception as e:
        print(f"❌ Erreur lors de l'insertion de {obj.object_name}: {e}")

print("✅ Données chargées dans Snowflake.")

cursor.close()
conn.close()
