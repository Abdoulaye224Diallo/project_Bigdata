import pandas as pd
import snowflake.connector
from sqlalchemy import create_engine

# --- Connexion à Snowflake ---
sf_conn = snowflake.connector.connect(
    user='DIALLO224',
    password='Efreiecole224@',
    account='uadrunu-tc91125',
    warehouse='COMPUTE_WH',
    database='DATA_LAKE',
    schema='PUBLIC'
)

# --- Lecture des données depuis Snowflake ---
def fetch_stationnement():
    query = """
        SELECT * FROM stationnement
        WHERE NOM_LIEU IS NOT NULL
          AND HORAIRES IS NOT NULL
    """
    df = pd.read_sql(query, sf_conn)
    return df

def fetch_sncf_data():
    query = "SELECT * FROM sncf_data"
    df = pd.read_sql(query, sf_conn)
    return df

# --- Connexion à PostgreSQL et envoi ---
def send_to_postgres(df, table_name):
    try:
        user = "postgres"
        password = "diallo"
        host = "localhost"
        port = "5432"
        db = "datamart"

        engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")
        df.to_sql(table_name, engine, if_exists='replace', index=False)

        print(f"✅ Données transférées avec succès dans PostgreSQL → table `{table_name}`")
    except Exception as e:
        print(f"❌ Erreur PostgreSQL (table {table_name}) : {e}")

# --- Main ---
def main():
    print("📥 Lecture des données 'stationnement' depuis Snowflake...")
    df_stationnement = fetch_stationnement()
    print(f"✔️ {len(df_stationnement)} lignes récupérées avec {len(df_stationnement.columns)} colonnes.")
    print("🚀 Envoi dans PostgreSQL...")
    send_to_postgres(df_stationnement, "stationnement_complet")

    print("\n📥 Lecture des données 'sncf_data' depuis Snowflake...")
    df_sncf = fetch_sncf_data()
    print(f"✔️ {len(df_sncf)} lignes récupérées avec {len(df_sncf.columns)} colonnes.")
    print("🚀 Envoi dans PostgreSQL...")
    send_to_postgres(df_sncf, "sncf_data")

if __name__ == "__main__":
    main()
