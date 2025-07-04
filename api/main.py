from fastapi import FastAPI, Form
from fastapi.responses import HTMLResponse, JSONResponse
import snowflake.connector
import pandas as pd
import mlflow.sklearn

app = FastAPI()

# Charger le modèle MLflow (modifie le chemin si besoin)
model = mlflow.sklearn.load_model(
    "file:///C:/Users/AbdoulayeDIALLO/Desktop/challenge_dataLake/mlruns/546630276834442325/models/m-188c004558c14aeb82925aca4a280ef0/artifacts"
)

def get_connection():
    return snowflake.connector.connect(
        user='DIALLO224',
        password='Efreiecole224@',
        account='uadrunu-tc91125',
        warehouse='COMPUTE_WH',
        database='DATA_LAKE',
        schema='PUBLIC'
    )

def get_all_lieux():
    conn = get_connection()
    query = "SELECT DISTINCT NOM_LIEU FROM stationnement WHERE NOM_LIEU IS NOT NULL"
    df = pd.read_sql(query, conn)
    conn.close()
    return df['NOM_LIEU'].dropna().unique().tolist()

@app.get("/", response_class=HTMLResponse)
async def get_form():
    lieux = get_all_lieux()
    lieux_options = "\n".join([f'<option value="{lieu}">' for lieu in lieux])

    horaires = [f"{str(h).zfill(2)}:{m:02d}" for h in range(24) for m in (0, 30)]
    horaires_html = "\n".join([f'<option value="{h}:{m:02d}">{h}:{m:02d}</option>' for h in range(24) for m in (0, 30)])

    return f"""
    <html>
        <head>
            <title>Prédiction du Trafic</title>
            <style>
                body {{
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                    background: #f0f2f5;
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    height: 100vh;
                    margin: 0;
                }}
                .container {{
                    background: white;
                    padding: 30px 40px;
                    border-radius: 12px;
                    box-shadow: 0 4px 15px rgba(0,0,0,0.1);
                    width: 350px;
                }}
                h2 {{
                    text-align: center;
                    color: #333;
                    margin-bottom: 25px;
                }}
                label {{
                    display: block;
                    margin-bottom: 6px;
                    font-weight: 600;
                    color: #555;
                }}
                input[list], select, input[type="submit"] {{
                    width: 100%;
                    padding: 10px;
                    margin-bottom: 20px;
                    border-radius: 6px;
                    border: 1px solid #ccc;
                    font-size: 16px;
                    transition: border-color 0.3s;
                }}
                input[list]:focus, select:focus {{
                    outline: none;
                    border-color: #007bff;
                    box-shadow: 0 0 5px rgba(0,123,255,0.5);
                }}
                input[type="submit"] {{
                    background-color: #007bff;
                    color: white;
                    border: none;
                    font-weight: 700;
                    cursor: pointer;
                    transition: background-color 0.3s;
                }}
                input[type="submit"]:hover {{
                    background-color: #0056b3;
                }}
                #result {{
                    margin-top: 20px;
                    font-weight: bold;
                    text-align: center;
                    min-height: 24px;
                    color: #222;
                }}
            </style>
            <script>
                async function submitForm(event) {{
                    event.preventDefault();
                    const formData = new FormData(event.target);
                    try {{
                        const response = await fetch('/predict_form', {{
                            method: 'POST',
                            body: formData
                        }});

                        if (!response.ok) {{
                            document.getElementById("result").innerHTML = "❌ Erreur serveur: " + response.status;
                            return;
                        }}

                        const result = await response.json();
                        document.getElementById("result").innerHTML = result.message;
                    }} catch (error) {{
                        document.getElementById("result").innerHTML = "❌ Erreur JS: " + error.message;
                    }}
                }}
            </script>
        </head>
        <body>
            <div class="container">
                <h2>Prédiction du trafic de stationnement 🚗</h2>
                <form onsubmit="submitForm(event)">
                    <label for="nom_lieu">Nom du lieu :</label>
                    <input list="lieux-list" name="nom_lieu" id="nom_lieu" required autocomplete="off"/>
                    <datalist id="lieux-list">
                        {lieux_options}
                    </datalist>

                    <label for="horaire">Horaire :</label>
                    <select name="horaire" id="horaire">
                        {horaires_html}
                    </select>

                    <input type="submit" value="Prédire">
                </form>
                <div id="result"></div>
            </div>
        </body>
    </html>
    """

@app.post("/predict_form")
async def predict_form(nom_lieu: str = Form(...), horaire: str = Form(...)):
    try:
        heure = int(horaire.split(":")[0])
    except:
        return JSONResponse(content={"message": "❌ Erreur : l'heure doit être au format HH:MM"})

    conn = get_connection()
    query = "SELECT NOM_LIEU FROM stationnement WHERE NOM_LIEU = %s"
    df = pd.read_sql(query, conn, params=(nom_lieu,))
    conn.close()

    if df.empty:
        return JSONResponse(content={"message": f"❌ Le lieu '{nom_lieu}' est inconnu."})

    all_lieux = get_all_lieux()
    lieux_cat = pd.Categorical([nom_lieu], categories=all_lieux)
    nom_lieu_code = lieux_cat.codes[0]

    X_input = pd.DataFrame({'nom_lieu_code': [nom_lieu_code], 'heure': [heure]})
    prediction = model.predict(X_input)[0]

    if prediction == 1:
        msg = f"🚦 Le trafic est probablement <strong>ÉLEVÉ</strong> à <em>{nom_lieu}</em> à {horaire}."
    else:
        msg = f"✅ Le trafic est probablement <strong>FAIBLE</strong> à <em>{nom_lieu}</em> à {horaire}."

    return JSONResponse(content={"message": msg})

# Optional : endpoint test POST simple
@app.post("/test")
async def test_endpoint(value: str = Form(...)):
    return {"message": f"Tu as envoyé : {value}"}
