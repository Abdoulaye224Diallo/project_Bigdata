from minio import Minio
import snowflake.connector
import json

# Connexion MinIO
minio_client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

bucket_name = "stationnement-bucket"

# Connexion Snowflake
conn = snowflake.connector.connect(
    user='DIALLO224',
    password='Efreiecole224@',
    account='uadrunu-tc91125',  # uniquement la partie avant .snowflakecomputing.com
    warehouse='COMPUTE_WH',
    database='DATA_LAKE',
    schema='PUBLIC'
)

cursor = conn.cursor()

# Fonction pour modifier les colonnes en VARCHAR si besoin
def alter_columns_to_varchar():
    # Liste des colonnes problématiques à convertir en VARCHAR(1000)
    cols_to_alter = ['nbre_pl', 'nbre_pmr', 'lumiere', 'duree', 'horaires', 'proprio', 'comm']
    for col in cols_to_alter:
        try:
            alter_sql = f"ALTER TABLE STATIONNEMENT ALTER COLUMN {col} SET DATA TYPE VARCHAR(1000)"
            cursor.execute(alter_sql)
            print(f"✅ Colonne {col} convertie en VARCHAR(1000)")
        except Exception as e:
            # Si la colonne n'existe pas ou autre erreur, on continue
            print(f"⚠️ Impossible de modifier la colonne {col} : {e}")

def clean_value(val, col_name=None):
    # Nettoie la valeur : si vide, None (NULL en SQL)
    if val is None:
        return None
    if isinstance(val, str):
        v = val.strip()
        if v == "" or v.lower() == "n/a":
            return None
        # Pour les colonnes numériques converties en texte, on force en string
        if col_name in ['nbre_pl', 'nbre_pmr', 'duree']:
            # Nettoyer les valeurs non numériques et forcer en string
            try:
                # Si la valeur est censée être un nombre mais contient du texte, on garde le texte tel quel
                float(v)
                return v  # numérique sous forme de string
            except:
                return v  # texte
        # Pour la colonne lumiere qui semble boolean, convertir True/False/strings
        if col_name == 'lumiere':
            # Traiter booléens et chaînes en string
            if v.lower() in ['true', 'false']:
                return v.lower()
            else:
                return v  # chaîne non booléenne
        return v
    # Si ce n'est pas une chaîne, retourner tel quel
    return val

# On convertit les colonnes problématiques avant insertion
alter_columns_to_varchar()

objects = minio_client.list_objects(bucket_name)

for obj in objects:
    print(f"Traitement de l'objet: {obj.object_name}")
    try:
        response = minio_client.get_object(bucket_name, obj.object_name)
        data_bytes = response.read()
        response.close()
        response.release_conn()

        data = json.loads(data_bytes)

        # Nettoyer et préparer les données
        if 'id' not in data:
            print(f"⚠️ Objet {obj.object_name} ignoré car pas de clé 'id'.")
            continue

        data_clean = {k: clean_value(v, k) for k, v in data.items()}

        columns = ', '.join(data_clean.keys())
        values_placeholders = ', '.join(['%s'] * len(data_clean))
        values = tuple(data_clean.values())

        merge_sql = f"""
        MERGE INTO STATIONNEMENT t
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

print("✅ Données nettoyées et chargées dans Snowflake avec suppression des doublons.")

cursor.close()
conn.close()
