import os
import time
import requests
import json
import hashlib
import traceback
from datetime import datetime, timezone, timedelta, date
from google.cloud import bigquery

client = bigquery.Client()

# 1. TABLES BIGQUERY
SOURCE_TABLE_ID = "les-fourcasters.imports_csv.departements_latitude_longitude"
DEST_TABLE_ID = "les-fourcasters.raw_database.raw_daily_weather" # Table isolée pour les données chaudes

# 2. VARIABLES MÉTÉO
VARIABLES_METEO = [
    "weather_code", "temperature_2m_mean", "temperature_2m_min", "temperature_2m_max",
    "apparent_temperature_mean", "apparent_temperature_min", "apparent_temperature_max",
    "relative_humidity_2m_mean", "relative_humidity_2m_min", "relative_humidity_2m_max",
    "dew_point_2m_mean", "precipitation_sum", "rain_sum", "snowfall_sum",
    "precipitation_hours", "wind_speed_10m_mean", "wind_speed_10m_max",
    "wind_gusts_10m_max", "wind_direction_10m_dominant", "cloud_cover_mean",
    "pressure_msl_mean", "sunshine_duration", "shortwave_radiation_sum",
    "et0_fao_evapotranspiration", "vapour_pressure_deficit_max",
    "soil_moisture_0_to_7cm_mean", "soil_moisture_7_to_28cm_mean",
    "soil_moisture_28_to_100cm_mean", "soil_temperature_0_to_7cm_mean"
]

def calculer_hash(row: dict) -> str:
    """Génère un hash unique pour la ligne, indépendant de l'ordre des colonnes."""
    row_str = json.dumps(row, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(row_str.encode()).hexdigest()

def enrichir_lignes(data: list) -> list:
    """Ajoute le timestamp d'insertion et le hash à chaque ligne."""
    now = datetime.now(timezone.utc).isoformat()
    enriched = []
    for row in data:
        row_copy = dict(row)
        row_copy["row_hash"] = calculer_hash(row)
        row_copy["inserted_at"] = now
        enriched.append(row_copy)
    return enriched

def charger_dans_bigquery(data: list):
    """Charge un lot de données dans BigQuery."""
    if not data:
        return
    data_enrichie = enrichir_lignes(data)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND"
    )
    load_job = client.load_table_from_json(data_enrichie, DEST_TABLE_ID, job_config=job_config)
    load_job.result()

def ingest_data():
    """Fonction principale d'ingestion appelée par le pipeline."""
    try:
        # 3. CRÉATION DE LA TABLE SI ELLE N'EXISTE PAS (Avec types sécurisés en FLOAT64)
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS `{DEST_TABLE_ID}` (
                Commune STRING,
                Latitude FLOAT64,
                Longitude FLOAT64,
                Region STRING,
                Departement STRING,
                Numero_Departement STRING,
                code_INSEE STRING,
                date DATE,
                row_hash STRING,
                inserted_at TIMESTAMP,
                weather_code INT64,
                temperature_2m_mean FLOAT64,
                temperature_2m_min FLOAT64,
                temperature_2m_max FLOAT64,
                apparent_temperature_mean FLOAT64,
                apparent_temperature_min FLOAT64,
                apparent_temperature_max FLOAT64,
                relative_humidity_2m_mean FLOAT64,
                relative_humidity_2m_min FLOAT64,
                relative_humidity_2m_max FLOAT64,
                dew_point_2m_mean FLOAT64,
                precipitation_sum FLOAT64,
                rain_sum FLOAT64,
                snowfall_sum FLOAT64,
                precipitation_hours FLOAT64,
                wind_speed_10m_mean FLOAT64,
                wind_speed_10m_max FLOAT64,
                wind_gusts_10m_max FLOAT64,
                wind_direction_10m_dominant FLOAT64,
                cloud_cover_mean FLOAT64,
                pressure_msl_mean FLOAT64,
                sunshine_duration FLOAT64,
                shortwave_radiation_sum FLOAT64,
                et0_fao_evapotranspiration FLOAT64,
                vapour_pressure_deficit_max FLOAT64,
                soil_moisture_0_to_7cm_mean FLOAT64,
                soil_moisture_7_to_28cm_mean FLOAT64,
                soil_moisture_28_to_100cm_mean FLOAT64,
                soil_temperature_0_to_7cm_mean FLOAT64
            )
        """
        client.query(create_table_query).result()

        end_date_str = date.today().strftime("%Y-%m-%d")

        # 4. RECHERCHE DES VILLES À METTRE À JOUR
        query_villes = f"""
            WITH all_villes AS (
                SELECT Commune, Latitude, Longitude, `Région`, Departement, Numero_Departement, `code INSEE` as code_insee
                FROM `{SOURCE_TABLE_ID}`
            )
            SELECT v.Commune, v.Latitude, v.Longitude, v.`Région`, v.Departement, v.Numero_Departement, v.code_insee,
                   d.max_date
            FROM all_villes v
            LEFT JOIN (
                SELECT Commune, MAX(date) as max_date 
                FROM `{DEST_TABLE_ID}` 
                GROUP BY Commune
            ) d ON v.Commune = d.Commune
            WHERE d.max_date IS NULL OR d.max_date < '{end_date_str}'
        """
        villes = list(client.query(query_villes).result())

        if not villes:
            print("Toutes les villes sont déjà à jour !")
            return

        print(f"[*] Villes restantes à traiter : {len(villes)}")

        batch_weather_data = []
        consecutive_rate_limits = 0

        # 5. BOUCLE DE RÉCUPÉRATION API
        for index, ville in enumerate(villes):
            commune_nom = ville["Commune"]
            derniere_date_bquot = ville["max_date"]
            
            if derniere_date_bquot:
                if isinstance(derniere_date_bquot, str):
                    dt_derniere = datetime.strptime(derniere_date_bquot, "%Y-%m-%d")
                else:
                    dt_derniere = derniere_date_bquot
                
                start_date_dt = dt_derniere + timedelta(days=1)
                start_date_str = start_date_dt.strftime("%Y-%m-%d")
            else:
                # La date de repli corrigée !
                start_date_str = "2026-08-01"

            url = "https://archive-api.open-meteo.com/v1/archive"
            
            # Les paramètres exacts demandés
            params = {
                "latitude": ville["Latitude"],
                "longitude": ville["Longitude"],
                "start_date": start_date_str,
                "end_date": end_date_str,
                "daily": ",".join(VARIABLES_METEO),
                "timezone": "Europe/Paris",
                "models": "era5_seamless"
            }
            
            while True:
                response = requests.get(url, params=params)
                try:
                    data = response.json()
                except:
                    data = {}

                if response.status_code == 200 and not data.get("error"):
                    consecutive_rate_limits = 0
                    daily_data = data.get("daily", {})
                    if "time" in daily_data:
                        for i in range(len(daily_data["time"])):
                            row = {
                                "Commune": str(ville["Commune"]) if ville["Commune"] is not None else None,
                                "Latitude": float(ville["Latitude"]) if ville["Latitude"] is not None else None,
                                "Longitude": float(ville["Longitude"]) if ville["Longitude"] is not None else None,
                                "Region": str(ville["Région"]) if ville["Région"] is not None else None,
                                "Departement": str(ville["Departement"]) if ville["Departement"] is not None else None,
                                "Numero_Departement": str(ville["Numero_Departement"]) if ville["Numero_Departement"] is not None else None,
                                "code_INSEE": str(ville["code_insee"]) if ville["code_insee"] is not None else None,
                                "date": daily_data["time"][i]
                            }

                            # On crée un drapeau pour vérifier s'il y a des données
                            has_valid_data = False
                            
                            for var in VARIABLES_METEO:
                                val = daily_data.get(var, [])[i] if daily_data.get(var) else None
                                row[var] = val
                                # Si au moins une variable n'est pas nulle, on lève le drapeau
                                if val is not None:
                                    has_valid_data = True
                                    
                            # On n'ajoute la ligne QUE si le drapeau est levé
                            if has_valid_data:
                                batch_weather_data.append(row)

                    print(f"-> OK : {commune_nom} (du {start_date_str} au {end_date_str})")
                    break
                    
                elif data.get("error") and "limit" in data.get("reason", "").lower():
                    consecutive_rate_limits += 1
                    if consecutive_rate_limits >= 6:
                        charger_dans_bigquery(batch_weather_data)
                        print("⚠️ Seuil de 6 limites atteint : sauvegarde d'urgence effectuée.")
                        return
                    print(f"🚦 Limite API atteinte. Pause de 61s... ({consecutive_rate_limits}/6)")
                    time.sleep(61)
                else:
                    print(f"❌ Erreur API ou pas de données pour {commune_nom}, passage.")
                    break
                    
            if len(batch_weather_data) >= 5000:
                charger_dans_bigquery(batch_weather_data)
                batch_weather_data = []

            time.sleep(3)

        charger_dans_bigquery(batch_weather_data)
        print("✅ Pipeline incrémental exécuté avec succès !")

    except Exception as e:
        error_detail = traceback.format_exc()
        print(f"🔥 ERREUR CRITIQUE : {error_detail}")

# 6. EXÉCUTION
if __name__ == "__main__":
    ingest_data()