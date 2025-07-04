import pandas as pd
import snowflake.connector

# Connexion à Snowflake
conn = snowflake.connector.connect(
    user='DIALLO224',
    password='Efreiecole224@',
    account='uadrunu-tc91125',
    warehouse='COMPUTE_WH',
    database='DATA_LAKE',
    schema='PUBLIC'
)

# Exécution de la requête
query = "SELECT * FROM STATIONNEMENT"
df = pd.read_sql(query, conn)

# Export en CSV
df.to_csv("export_stationnement.csv", index=False)
