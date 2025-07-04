import snowflake.connector
import pandas as pd
import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report

# Connexion à Snowflake
def get_connection():
    return snowflake.connector.connect(
        user='DIALLO224',
        password='Efreiecole224@',
        account='uadrunu-tc91125',
        warehouse='COMPUTE_WH',
        database='DATA_LAKE',
        schema='PUBLIC'
    )

# Récupération des données
def fetch_data():
    query = """
    SELECT NOM_LIEU, HORAIRES, NBRE_PL
    FROM stationnement
    WHERE NOM_LIEU IS NOT NULL AND HORAIRES IS NOT NULL AND NBRE_PL IS NOT NULL
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Prétraitement
def preprocess_data(df):
    df['heure'] = df['HORAIRES'].str.extract(r'(\d{1,2})').astype(float)
    df.dropna(subset=['heure'], inplace=True)

    seuil = df['NBRE_PL'].median()
    df['trafic_eleve'] = (df['NBRE_PL'] > seuil).astype(int)

    df['nom_lieu_code'] = df['NOM_LIEU'].astype('category').cat.codes

    X = df[['nom_lieu_code', 'heure']]
    y = df['trafic_eleve']
    return X, y, df

# 🔍 Affichage des lieux avec trafic élevé/faible et heure moyenne
def show_top_trafic_stats(df):
    grouped = df.groupby('NOM_LIEU').agg({
        'NBRE_PL': 'mean',
        'heure': 'mean'
    }).sort_values(by='NBRE_PL', ascending=False)

    print("\n🚦 Lieux avec le trafic MOYEN le plus ÉLEVÉ :")
    for lieu, row in grouped.head(5).iterrows():
        print(f"🟥 {lieu} → {row['NBRE_PL']:.1f} places, heure moyenne : {int(row['heure']):02d}:00")

    print("\n🟢 Lieux avec le trafic MOYEN le plus FAIBLE :")
    for lieu, row in grouped.tail(5).iterrows():
        print(f"🟩 {lieu} → {row['NBRE_PL']:.1f} places, heure moyenne : {int(row['heure']):02d}:00")

# Entraînement et enregistrement MLflow
def train_and_log_model():
    df = fetch_data()
    X, y, df_preprocessed = preprocess_data(df)

    # 📊 Statistiques
    show_top_trafic_stats(df_preprocessed)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    acc = accuracy_score(y_test, y_pred)

    print("\n📈 Rapport de classification :\n", classification_report(y_test, y_pred))
    print(f"🎯 Accuracy: {acc:.4f}")

    mlflow.set_experiment("Parking_Traffic_Prediction")

    with mlflow.start_run():
        mlflow.sklearn.log_model(model, "parking_traffic_model")
        mlflow.log_param("model_type", "RandomForest")
        mlflow.log_metric("accuracy", acc)

        run_id = mlflow.active_run().info.run_id
        print(f"\n✅ Modèle enregistré avec run_id : {run_id}")
        return run_id

if __name__ == "__main__":
    train_and_log_model()
