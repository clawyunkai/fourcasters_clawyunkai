import os
import time
import requests
import json
import hashlib
from datetime import datetime, timezone
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "fourcasters_cle.json"
client = bigquery.Client()

SOURCE_TABLE_ID = "les-fourcasters.imports_csv.departements_latitude_longitude"
DEST_TABLE_ID = "les-fourcasters.raw_database.open_meteo"

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
    row_str = json.dumps(row, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(row_str.encode()).hexdigest()

def enrichir_lignes(data: list) -> list:
    now = datetime.now(timezone.utc).isoformat()
    enriched = []
    for row in data:
        row_copy = dict(row)
        row_copy["row_hash"] = calculer_hash(row)
        row_copy["inserted_at"] = now
        enriched.append(row_copy)
    return enriched

def charger_dans_bigquery(data: list):
    """Envoie les données accumulées dans BigQuery"""
    if not data:
        print("[!] Aucune donnée à charger.")
        return

    print(f"[*] Enrichissement de {len(data)} lignes (calcul des hash)...")
    data_enrichie = enrichir_lignes(data)

    print(f"[*] Envoi des données vers la table BigQuery {DEST_TABLE_ID}...")
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True,
    )
    load_job = client.load_table_from_json(data_enrichie, DEST_TABLE_ID, job_config=job_config)
    load_job.result()
    print(f"🎉 SUCCÈS : {len(data_enrichie)} lignes insérées dans BigQuery !")

def main():
    print("--- DÉMARRAGE DU PIPELINE MÉTÉO (CLOUD READY) ---")
    
    query_villes = f"""
        SELECT Commune, Latitude, Longitude, `Région`, Departement, Numero_Departement, `code INSEE` as code_insee
        FROM `{SOURCE_TABLE_ID}`
    """
    villes = list(client.query(query_villes).result())
    print(f"[+] {len(villes)} villes trouvées.")

    all_weather_data = []
    start_date_str = "2000-01-01"
    end_date_str = "2026-08-01"

    consecutive_rate_limits = 0  # Compteur pour les limites d'API

    for index, ville in enumerate(villes):
        print(f"    -> Traitement de la ville {index + 1}/{len(villes)} : {ville['Commune']}...", end="\r")
        
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": ville["Latitude"],
            "longitude": ville["Longitude"],
            "start_date": start_date_str,
            "end_date": end_date_str,
            "daily": ",".join(VARIABLES_METEO),
            "timezone": "auto"
        }
        
        while True:
            response = requests.get(url, params=params)
            try:
                data = response.json()
            except:
                data = {}

            if response.status_code == 200 and not data.get("error"):
                # Succès ! On remet le compteur à zéro
                consecutive_rate_limits = 0
                daily_data = data.get("daily", {})
                if "time" in daily_data:
                    for i in range(len(daily_data["time"])):
                        row = {
                            "Commune": ville["Commune"],
                            "Latitude": ville["Latitude"],
                            "Longitude": ville["Longitude"],
                            "Region": ville["Région"],
                            "Departement": ville["Departement"],
                            "Numero_Departement": ville["Numero_Departement"],
                            "code_INSEE": ville["code_insee"],
                            "date": daily_data["time"][i]
                        }
                        for var in VARIABLES_METEO:
                            row[var] = daily_data.get(var, [])[i] if daily_data.get(var) else None
                        all_weather_data.append(row)
                break
                
            elif data.get("error") and "limit" in data.get("reason", "").lower():
                consecutive_rate_limits += 1
                print(f"\n[!] Limite API atteinte ({consecutive_rate_limits}/6) pour {ville['Commune']}.")
                
                # S'il y a déjà eu 6 blocages consécutifs : on sauvegarde et on quitte proprement
                if consecutive_rate_limits >= 6:
                    print("\n[!] Seuil critique de 6 blocages atteint. Sauvegarde d'urgence et arrêt du script.")
                    print("[*] Le Cloud Scheduler relancera le script à l'heure suivante pour reprendre la suite.")
                    charger_dans_bigquery(all_weather_data)
                    return
                
                print("    -> Pause de 61 secondes avant de réessayer...")
                time.sleep(61)
            else:
                print(f"\n[!] Erreur inattendue pour {ville['Commune']} : {response.text}")
                break
                
        time.sleep(1)

    # Si la boucle va jusqu'au bout sans encombre
    charger_dans_bigquery(all_weather_data)

if __name__ == "__main__":
    main()