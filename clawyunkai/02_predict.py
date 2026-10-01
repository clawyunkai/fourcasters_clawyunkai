"""02_predict.py — Prédit le risque de canicule de l'été à venir (2 modèles) et l'écrit dans BigQuery."""
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()  # charge GOOGLE_APPLICATION_CREDENTIALS depuis le .env

PROJECT = "les-fourcasters"
MARTS = "dbt_dev_marts"                 # dataset produit par dbt
FEATURES = "canicule_features"          # table de features (modèle dbt)
ML = "machine_learning"                 # dataset où on écrit les prédictions
TABLE_PREDICTIONS = "predictions_canicule"

# Chemins des modèles, relatifs à CE script (fonctionne quel que soit le dossier d'où on le lance)
DOSSIER_MODELES = Path(__file__).parent / "00_Archive"
MODELE_EVENEMENT = DOSSIER_MODELES / "modele_canicule_evenement.joblib"   # classification
MODELE_NB_JOUR = DOSSIER_MODELES / "modele_canicule_nb_jour.joblib"       # régression

ANNEE = datetime.now().year             # été à prédire (features de sept. N-1 à mai N)
SEUIL_ALERTE = 0.5                      # seuil pour dériver un oui/non de la probabilité


def predire():
    client = bigquery.Client(project=PROJECT)

    # 1) LIRE une seule fois les features de l'année à prédire
    df = client.query(f"""
        SELECT *
        FROM `{PROJECT}.{MARTS}.{FEATURES}`
        WHERE annee_reference_canicule = {ANNEE}
    """).to_dataframe(create_bqstorage_client=False)

    if df.empty:
        print(f"Aucune feature pour {ANNEE} : lancer dbt avant la prédiction.")
        return

    # 2) CHARGER les deux pipelines entraînés
    modele_evenement = joblib.load(MODELE_EVENEMENT)
    modele_nb_jour = joblib.load(MODELE_NB_JOUR)

    resultat = df[["code_departement", "annee_reference_canicule"]].copy()

    # 3a) Modèle 1 (classification) : probabilité d'au moins une canicule
    colonnes = list(modele_evenement.feature_names_in_)   # ses propres colonnes d'entraînement
    resultat["proba_canicule"] = modele_evenement.predict_proba(df[colonnes])[:, 1].round(3)
    resultat["alerte_canicule"] = resultat["proba_canicule"] >= SEUIL_ALERTE

    # 3b) Modèle 2 (régression) : nombre de jours, jamais négatif
    colonnes = list(modele_nb_jour.feature_names_in_)
    resultat["nb_jours_predits"] = np.clip(modele_nb_jour.predict(df[colonnes]), 0, None).round(1)

    resultat["date_prediction"] = datetime.now(timezone.utc)   # pour garder l'historique

    # 4) ÉCRIRE une seule table : une ligne par département, toutes les prédictions côte à côte
    client.load_table_from_dataframe(
        resultat,
        f"{PROJECT}.{ML}.{TABLE_PREDICTIONS}",
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
    ).result()
    print(f"{len(resultat)} prédictions ajoutées dans {ML}.{TABLE_PREDICTIONS}")


if __name__ == "__main__":
    predire()