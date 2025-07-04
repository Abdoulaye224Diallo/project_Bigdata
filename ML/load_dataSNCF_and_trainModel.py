import pandas as pd
import snowflake.connector
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import mlflow
import mlflow.sklearn

# Connexion Snowflake
conn = snowflake.connector.connect(
    user='DIALLO224',
    password='Efreiecole224@',
    account='uadrunu-tc91125',
    warehouse='COMPUTE_WH',
    database='DATA_LAKE',
    schema='PUBLIC'
)

# Lecture des données
query = "SELECT * FROM SNCF_DATA"
df = pd.read_sql(query, conn)
conn.close()

# Conversion de la date
def convert_date_column(df, col='DATE'):
    if pd.api.types.is_numeric_dtype(df[col]):
        df[col] = pd.to_datetime(df[col], unit='ms')
    else:
        df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')
    return df

df = convert_date_column(df, 'DATE')

# Features temporelles
df['MOIS'] = df['DATE'].dt.month
df['ANNEE'] = df['DATE'].dt.year

# Cible binaire
df['PONCTUEL'] = (df['TAUX_DE_PONCTUALITE'] >= 80).astype(int)

# Encodage
df['NOM_DE_LA_LIGNE_CODE'] = df['NOM_DE_LA_LIGNE'].astype('category').cat.codes
df['SERVICE_CODE'] = df['SERVICE'].astype('category').cat.codes
df['LIGNE_CODE'] = df['LIGNE'].astype('category').cat.codes

# Features
feature_cols = [
    'MOIS', 'ANNEE',
    'SERVICE_CODE', 'LIGNE_CODE',
    'NOM_DE_LA_LIGNE_CODE',
    'NOMBRE_DE_VOYAGEURS_A_L_HEURE_POUR_UN_VOYAGEUR_EN_RETARD'
]

X = df[feature_cols]
y = df['PONCTUEL']

# Split des données
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Tracking URI locale (modifie si tu utilises un serveur distant)
mlflow.set_tracking_uri("http://localhost:5000")
mlflow.set_experiment("Ponctualité_SNCF_Traffic")

# Entraînement et log MLflow
with mlflow.start_run() as run:
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    report = classification_report(y_test, y_pred)
    print("Rapport classification :\n", report)

    # Log
    mlflow.sklearn.log_model(model, artifact_path="random_forest_model")
    mlflow.log_param("n_estimators", 100)
    mlflow.log_param("random_state", 42)
    mlflow.log_metric("accuracy", model.score(X_test, y_test))

    print("✅ Entraînement terminé.")
    print(f"Run ID : {run.info.run_id}")
    print(f"Chemin du modèle : mlruns/0/{run.info.run_id}/artifacts/random_forest_model")
