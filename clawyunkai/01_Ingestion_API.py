import os
import time
import requests
import json
import hashlib
import traceback
from datetime import datetime, timezone, timedelta, date
from dotenv import load_dotenv
from google.cloud import bigquery
from src.chargement import calculer_hash, enrichir_lignes, charger_dans_bigquery
import logging

logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler("pipeline.log", encoding="utf-8"),
            logging.StreamHandler(),
        ],
)
logger=logging.getLogger(__name__)

# Charge les variables du fichier .env de votre ordinateur
load_dotenv()

# Le client BigQuery va maintenant trouver la clé tout seul
client = bigquery.Client()

# 1. TABLES BIGQUERY
SOURCE_TABLE_DEPARTEMENTS_ID = "les-fourcasters.imports_csv.departements_latitude_longitude"
DEST_TABLE_OPEN_METEO_ID = "les-fourcasters.raw_database.raw_daily_weather" # Table isolée pour les données chaudes de open_meteo
DEST_TABLE_ODISSE_CANICULE_DECES_DEPARTEMENT= "les-fourcasters.raw_database.odisse_canicule_deces_departement"
DEST_TABLE_ODISSE_CANICULE_SEVERITE_DEPARTEMENT= "les-fourcasters.raw_database.odisse_canicule_severite_departement"
DEST_TABLE_ODISSE_CANICULE_NB_JOURS_DEPARTEMENT= "les-fourcasters.raw_database.odisse_canicule_nb_jours_departement"
DEST_TABLE_ODISSE_CANICULE_POPULATION_CONCERNE_DEPARTEMENT= "les-fourcasters.raw_database.odisse_canicule_population_concerne_departement"
DEST_TABLE_ODISSE_CANICULE_DECES_ATTRIBUABLE_CONCERNE_DEPARTEMENT= "les-fourcasters.raw_database.odisse_canicule_deces_attribuable_departement"

# Date de repli sortie en constance globale
DEFAULT_START_DATE = "2026-08-01"

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

# Calcul d'un batch_id
batch_id=datetime.now(timezone.utc).strftime("%Y%m%D_%H%M%S")

# Requête API Open-meteo
def ingest_data_open_meteo():
    """Fonction principale d'ingestion pour open-meteo appelée par le pipeline."""

    #print(f"Debut de l'ingestion pour les données météo, lot {batch_id}")
    logging.info(f"Debut de l'ingestion pour les données météo, lot {batch_id}")

    try:
        # 3. CRÉATION DE LA TABLE SI ELLE N'EXISTE PAS (Avec types sécurisés en FLOAT64)
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS `{DEST_TABLE_OPEN_METEO_ID }` (
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
                FROM `{SOURCE_TABLE_DEPARTEMENTS_ID}`
            )

            SELECT v.Commune, v.Latitude, v.Longitude, v.`Région`, v.Departement, v.Numero_Departement, v.code_insee,
                   d.max_date
            FROM all_villes v
            LEFT JOIN (
                SELECT Commune, MAX(date) as max_date 
                FROM `{DEST_TABLE_OPEN_METEO_ID }` 
                GROUP BY Commune
            ) d ON v.Commune = d.Commune
            WHERE d.max_date IS NULL OR d.max_date < '{end_date_str}'
        """
        villes = list(client.query(query_villes).result())

        if not villes:
            #print("Toutes les villes sont déjà à jour !")
            logger
            return

        #print(f"[*] Villes restantes à traiter : {len(villes)}")
        logger.info(f"Villes restantes à traiter : {len(villes)}")

        batch_weather_data = []
        consecutive_rate_limits = 0

        # 5. BOUCLE DE RÉCUPÉRATION API
        for index, ville in enumerate(villes):
            commune_nom = ville["Commune"]
            derniere_date_bquot = ville["max_date"]
            
            if derniere_date_bquot:
                if isinstance(derniere_date_bquot, str): # Vérification si la date est du texte
                    dt_derniere = datetime.strptime(derniere_date_bquot, "%Y-%m-%d")
                else:
                    dt_derniere = derniere_date_bquot
                
                start_date_dt = dt_derniere + timedelta(days=1)
                start_date_str = start_date_dt.strftime("%Y-%m-%d")
            else:
                # La date de repli!
                start_date_str = DEFAULT_START_DATE

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
                    # 1. On impose un délai maximum de 15 secondes
                    response = requests.get(url, params=params, timeout=15)
                    response.raise_for_status() # Lève une erreur si le statut HTTP n'est pas 200
                    data = response.json()
                    
                except requests.exceptions.Timeout:
                    # 2. Si le serveur bloque, on patiente 5s et on recommence
                    logger.warning(f"⏳ Timeout API pour {commune_nom}, nouvelle tentative dans 5s...")
                    time.sleep(5)
                    continue 
                    
                except requests.exceptions.RequestException as e:
                    # 3. Si l'API est vraiment hors ligne, on passe à la ville suivante sans crasher
                    logger.error(f"❌ Erreur réseau critique pour {commune_nom}: {e}")
                    break
                
                except ValueError:
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

                            for var in VARIABLES_METEO:
                                var_list = daily_data.get(var)
                                val = var_list[i] if var_list and i < len(var_list) else None
                                row[var] = val  
                                    
                            # On n'ajoute la ligne QUE si la température est réellement présente
                            if any(row.get(var) is not None for var in VARIABLES_METEO):
                                batch_weather_data.append(row)

                    #print(f"-> OK : {commune_nom} (du {start_date_str} au {end_date_str})")
                    logger.info(f"-> OK : {commune_nom} (du {start_date_str} au {end_date_str})")
                    break
                    
                elif data.get("error") and "limit" in data.get("reason", "").lower():
                    consecutive_rate_limits += 1
                    if consecutive_rate_limits >= 6:
                        charger_dans_bigquery(batch_weather_data)
                        #print("⚠️ Seuil de 6 limites atteint : sauvegarde d'urgence effectuée.")
                        logger.warning(f"⚠️ Seuil de 6 limites atteint : sauvegarde d'urgence effectuée.")
                        return
                    #print(f"🚦 Limite API atteinte. Pause de 61s... ({consecutive_rate_limits}/6)")
                    logger.warning(f"🚦 Limite API atteinte. Pause de 61s... ({consecutive_rate_limits}/6)")
                    time.sleep(61)
                else:
                    #print(f"❌ Erreur API ou pas de données pour {commune_nom}, passage.")
                    logger.error(f"❌ Erreur API ou pas de données pour {commune_nom}, passage.")
                    break
                    
            if len(batch_weather_data) >= 5000:
                charger_dans_bigquery(batch_weather_data,DEST_TABLE_OPEN_METEO_ID )
                batch_weather_data = []

            time.sleep(3)

        charger_dans_bigquery(batch_weather_data, DEST_TABLE_OPEN_METEO_ID)
        #print("✅ Pipeline incrémental exécuté avec succès !")
        logger.info(f"✅ Pipeline incrémental exécuté avec succès !")

    except Exception as e:
        error_detail = traceback.format_exc()
        #print(f"🔥 ERREUR CRITIQUE : {error_detail}")
        logger.critical(f"🔥 ERREUR CRITIQUE : {error_detail}")
        raise

def ingest_data_odisse_canicule_deces():
    """Fonction d'ingestion pour les données des surplus de décés par département par année depuis l'API Odissé"""
    try:
                # 3. CRÉATION DE LA TABLE SI ELLE N'EXISTE PAS (Avec types sécurisés en FLOAT64)
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS `{DEST_TABLE_ODISSE_CANICULE_DECES_DEPARTEMENT}` (
                region STRING,
                region_Id INT64,
                departement STRING,
                numero_departement STRING,
                annee INT64,
                classe_age STRING,
                exces INTEGER,
                exces_relatif FLOAT64,
                row_hash STRING,
                inserted_at TIMESTAMP
            )
        """
        client.query(create_table_query).result()

        query_departement=f"""
            Select distinct 
                Numero_Departement
            FROM `{SOURCE_TABLE_DEPARTEMENTS_ID}`
        """
        numero_departement = list(client.query(query_departement).result())

        batch_odisse_canicule_deces_departement_data=[]

        for num_departement in (numero_departement):
            url = "https://odisse.santepubliquefrance.fr/api/explore/v2.1/catalog/datasets/canicules-exces-de-deces-pendant-les-vagues-de-chaleur-departement/records"
            code_dep = num_departement["Numero_Departement"]
            # Les paramètres exacts demandés
            params = {
                "lang": "fr",
                "limit": 100,
                "offset": 0,
                "where": f'dep="{code_dep}"'
            }

            while True:
                response = requests.get(url, params=params)
                try:
                    data = response.json()
                except ValueError:
                    data = {}

                if response.status_code == 200 and not data.get("error"):
                    consecutive_rate_limits = 0

                    for item in data.get("results",[]):
                        row={
                            "region": item.get("reglib"),
                            "region_Id": item.get("reg"),
                            "departement": item.get("libgeo"),
                            "numero_departement": item.get("dep"),
                            "annee": item.get("annee"),
                            "classe_age": item.get("classe_age"),
                            "exces": item.get("exces"),
                            "exces_relatif": item.get("exces_relatif") 
                        }

                        batch_odisse_canicule_deces_departement_data.append(row)

                    print(f"-> OK pour département {code_dep}")
                    break

                elif data.get("error") and "limit" in data.get("reason", "").lower():
                    consecutive_rate_limits += 1
                    if consecutive_rate_limits >= 6:
                        charger_dans_bigquery(batch_odisse_canicule_deces_departement_data)
                        print("⚠️ Seuil de 6 limites atteint : sauvegarde d'urgence effectuée.")
                        return
                    print(f"🚦 Limite API atteinte. Pause de 61s... ({consecutive_rate_limits}/6)")
                    time.sleep(61)
                else:
                    print(f"❌ Erreur API ou pas de données pour departement {num_departement}, passage.")
                    break

            if len(batch_odisse_canicule_deces_departement_data) >= 1000:
                charger_dans_bigquery(batch_odisse_canicule_deces_departement_data, DEST_TABLE_ODISSE_CANICULE_DECES_DEPARTEMENT)
                batch_odisse_canicule_deces_departement_data = []

            time.sleep(3)
        
        charger_dans_bigquery(batch_odisse_canicule_deces_departement_data, DEST_TABLE_ODISSE_CANICULE_DECES_DEPARTEMENT)
        print("✅ Pipeline incrémental exécuté avec succès !")

    except Exception as e:
        error_detail = traceback.format_exc()
        print(f"🔥 ERREUR CRITIQUE : {error_detail}")
        raise

def ingest_data_odisse_canicule_nb_jour():
    """Fonction d'ingestion pour les données du nombre de jours de canicules par département par année depuis l'API Odissé"""
    try:
                # 3. CRÉATION DE LA TABLE SI ELLE N'EXISTE PAS (Avec types sécurisés en FLOAT64)
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS `{DEST_TABLE_ODISSE_CANICULE_NB_JOURS_DEPARTEMENT}` (
                region STRING,
                region_Id INT64,
                departement STRING,
                numero_departement STRING,
                annee INT64,
                nb_jour_canicules FLOAT64,
                row_hash STRING,
                inserted_at TIMESTAMP
            )
        """
        client.query(create_table_query).result()

        query_departement=f"""
            Select distinct 
                Numero_Departement
            FROM `{SOURCE_TABLE_DEPARTEMENTS_ID}`
        """
        numero_departement = list(client.query(query_departement).result())

        batch_odisse_canicule_data=[]
        print("Lancement de l'ingestion pour les données du nombre de jours de canicules par département par année depuis l'API Odissé")

        for num_departement in (numero_departement):
            url = "https://odisse.santepubliquefrance.fr/api/explore/v2.1/catalog/datasets/canicules-nombres-de-jours-de-canicule-departement/records"
            code_dep = num_departement["Numero_Departement"]
            # Les paramètres exacts demandés
            params = {
                "lang": "fr",
                "limit": 100,
                "offset": 0,
                "where": f'dep="{code_dep}"'
            }

            while True:
                response = requests.get(url, params=params)
                try:
                    data = response.json()
                except ValueError:
                    data = {}

                if response.status_code == 200 and not data.get("error"):
                    consecutive_rate_limits = 0

                    for item in data.get("results",[]):
                        row={
                            "region": item.get("reglib"),
                            "region_Id": item.get("reg"),
                            "departement": item.get("libgeo"),
                            "numero_departement": item.get("dep"),
                            "annee": item.get("annee"),
                            "nb_jour_canicules": item.get("nb_j_can")
                        }

                        batch_odisse_canicule_data.append(row)

                    print(f"-> OK pour département {code_dep}")
                    break

                elif data.get("error") and "limit" in data.get("reason", "").lower():
                    consecutive_rate_limits += 1
                    if consecutive_rate_limits >= 6:
                        charger_dans_bigquery(batch_odisse_canicule_data)
                        print("⚠️ Seuil de 6 limites atteint : sauvegarde d'urgence effectuée.")
                        return
                    print(f"🚦 Limite API atteinte. Pause de 61s... ({consecutive_rate_limits}/6)")
                    time.sleep(61)
                else:
                    print(f"❌ Erreur API ou pas de données pour departement {num_departement}, passage.")
                    break

            if len(batch_odisse_canicule_data) >= 1000:
                charger_dans_bigquery(batch_odisse_canicule_data, DEST_TABLE_ODISSE_CANICULE_NB_JOURS_DEPARTEMENT)
                batch_odisse_canicule_data = []

            time.sleep(3)
        
        charger_dans_bigquery(batch_odisse_canicule_data, DEST_TABLE_ODISSE_CANICULE_NB_JOURS_DEPARTEMENT)
        print("✅ Pipeline incrémental exécuté avec succès !")

    except Exception as e:
        error_detail = traceback.format_exc()
        print(f"🔥 ERREUR CRITIQUE : {error_detail}")
        raise

def ingest_data_odisse_canicule_taille_population():
    """Fonction d'ingestion pour les données de la taille de la population concernées par la canicule par département par année depuis l'API Odissé"""
    try:
                # 3. CRÉATION DE LA TABLE SI ELLE N'EXISTE PAS (Avec types sécurisés en FLOAT64)
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS `{DEST_TABLE_ODISSE_CANICULE_POPULATION_CONCERNE_DEPARTEMENT}` (
                region STRING,
                region_Id INT64,
                departement STRING,
                numero_departement STRING,
                annee INT64,
                population_exposee FLOAT64,
                row_hash STRING,
                inserted_at TIMESTAMP
            )
        """
        client.query(create_table_query).result()

        query_departement=f"""
            Select distinct 
                Numero_Departement
            FROM `{SOURCE_TABLE_DEPARTEMENTS_ID}`
        """
        numero_departement = list(client.query(query_departement).result())

        batch_odisse_canicule_data=[]

        print(f"Lancement de l'ingestion de pour les données de la taille de la population concernées par la canicule par département par année depuis l'API Odissé")

        for num_departement in (numero_departement):
            url = "https://odisse.santepubliquefrance.fr/api/explore/v2.1/catalog/datasets/canicules-taille-de-la-population-concernee-par-une-canicule-departement/records"
            code_dep = num_departement["Numero_Departement"]
            # Les paramètres exacts demandés
            params = {
                "lang": "fr",
                "limit": 100,
                "offset": 0,
                "where": f'dep="{code_dep}"'
            }

            while True:
                response = requests.get(url, params=params)
                try:
                    data = response.json()
                except ValueError:
                    data = {}

                if response.status_code == 200 and not data.get("error"):
                    consecutive_rate_limits = 0

                    for item in data.get("results",[]):
                        row={
                            "region": item.get("reglib"),
                            "region_Id": item.get("reg"),
                            "departement": item.get("libgeo"),
                            "numero_departement": item.get("dep"),
                            "annee": item.get("annee"),
                            "population_exposee": item.get("pop_exposee")
                        }

                        batch_odisse_canicule_data.append(row)

                    print(f"-> OK pour département {code_dep}")
                    break

                elif data.get("error") and "limit" in data.get("reason", "").lower():
                    consecutive_rate_limits += 1
                    if consecutive_rate_limits >= 6:
                        charger_dans_bigquery(batch_odisse_canicule_data)
                        print("⚠️ Seuil de 6 limites atteint : sauvegarde d'urgence effectuée.")
                        return
                    print(f"🚦 Limite API atteinte. Pause de 61s... ({consecutive_rate_limits}/6)")
                    time.sleep(61)
                else:
                    print(f"❌ Erreur API ou pas de données pour departement {num_departement}, passage.")
                    break

            if len(batch_odisse_canicule_data) >= 1000:
                charger_dans_bigquery(batch_odisse_canicule_data, DEST_TABLE_ODISSE_CANICULE_POPULATION_CONCERNE_DEPARTEMENT)
                batch_odisse_canicule_data = []

            time.sleep(3)
        
        charger_dans_bigquery(batch_odisse_canicule_data, DEST_TABLE_ODISSE_CANICULE_POPULATION_CONCERNE_DEPARTEMENT)
        print("✅ Pipeline incrémental exécuté avec succès !")

    except Exception as e:
        error_detail = traceback.format_exc()
        print(f"🔥 ERREUR CRITIQUE : {error_detail}")
        raise

def ingest_data_odisse_canicule_severite():
    """Fonction d'ingestion pour les données de la sévérité de la canicule par département par année depuis l'API Odissé"""
    try:
                # 3. CRÉATION DE LA TABLE SI ELLE N'EXISTE PAS (Avec types sécurisés en FLOAT64)
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS `{DEST_TABLE_ODISSE_CANICULE_SEVERITE_DEPARTEMENT}` (
                region STRING,
                region_Id INT64,
                departement STRING,
                numero_departement STRING,
                annee INT64,
                severite FLOAT64,
                row_hash STRING,
                inserted_at TIMESTAMP
            )
        """
        client.query(create_table_query).result()

        query_departement=f"""
            Select distinct 
                Numero_Departement
            FROM `{SOURCE_TABLE_DEPARTEMENTS_ID}`
        """
        numero_departement = list(client.query(query_departement).result())

        batch_odisse_canicule_data=[]

        print(f"Lancement de l'ingestion pour les données de la sévérité de la canicule par département par année depuis l'API Odissé")

        for num_departement in (numero_departement):
            url = "https://odisse.santepubliquefrance.fr/api/explore/v2.1/catalog/datasets/canicules-severite-departement/records"
            code_dep = num_departement["Numero_Departement"]
            # Les paramètres exacts demandés
            params = {
                "lang": "fr",
                "limit": 100,
                "offset": 0,
                "where": f'dep="{code_dep}"'
            }

            while True:
                response = requests.get(url, params=params)
                try:
                    data = response.json()
                except ValueError:
                    data = {}

                if response.status_code == 200 and not data.get("error"):
                    consecutive_rate_limits = 0

                    for item in data.get("results",[]):
                        row={
                            "region": item.get("reglib"),
                            "region_Id": item.get("reg"),
                            "departement": item.get("libgeo"),
                            "numero_departement": item.get("dep"),
                            "annee": item.get("annee"),
                            "severite": item.get("severite")
                        }

                        batch_odisse_canicule_data.append(row)

                    print(f"-> OK pour département {code_dep}")
                    break

                elif data.get("error") and "limit" in data.get("reason", "").lower():
                    consecutive_rate_limits += 1
                    if consecutive_rate_limits >= 6:
                        charger_dans_bigquery(batch_odisse_canicule_data)
                        print("⚠️ Seuil de 6 limites atteint : sauvegarde d'urgence effectuée.")
                        return
                    print(f"🚦 Limite API atteinte. Pause de 61s... ({consecutive_rate_limits}/6)")
                    time.sleep(61)
                else:
                    print(f"❌ Erreur API ou pas de données pour departement {num_departement}, passage.")
                    break

            if len(batch_odisse_canicule_data) >= 1000:
                charger_dans_bigquery(batch_odisse_canicule_data, DEST_TABLE_ODISSE_CANICULE_SEVERITE_DEPARTEMENT)
                batch_odisse_canicule_data = []

            time.sleep(3)
        
        charger_dans_bigquery(batch_odisse_canicule_data, DEST_TABLE_ODISSE_CANICULE_SEVERITE_DEPARTEMENT)
        print("✅ Pipeline incrémental exécuté avec succès !")

    except Exception as e:
        error_detail = traceback.format_exc()
        print(f"🔥 ERREUR CRITIQUE : {error_detail}")
        raise

def ingest_data_odisse_canicule_deces_attribuable_CharleurCanicule():
    """Fonction d'ingestion pour les données des décès attribuable à la chaleur ou canicule par département par année depuis l'API Odissé"""
    try:
                # 3. CRÉATION DE LA TABLE SI ELLE N'EXISTE PAS (Avec types sécurisés en FLOAT64)
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS `{DEST_TABLE_ODISSE_CANICULE_DECES_ATTRIBUABLE_CONCERNE_DEPARTEMENT}` (
                region STRING,
                region_Id INT64,
                departement STRING,
                numero_departement STRING,
                annee INT64,
                classe_age STRING,
                deces_chaleur FLOAT64,
                deces_chaleur_inf FLOAT64,
                deces_chaleur_sup FLOAT64,
                fraction_deces_chaleur FLOAT64,
                fraction_deces_chaleur_inf FLOAT64,
                fraction_deces_chaleur_sup FLOAT64,
                deces_canicule FLOAT64,
                deces_canicule_inf FLOAT64,
                deces_canicule_sup FLOAT64,
                fraction_deces_canicule FLOAT64,
                fraction_deces_canicule_inf FLOAT64,
                fraction_deces_canicule_sup FLOAT64,
                row_hash STRING,
                inserted_at TIMESTAMP
            )
        """
        client.query(create_table_query).result()

        query_departement=f"""
            Select distinct 
                Numero_Departement
            FROM `{SOURCE_TABLE_DEPARTEMENTS_ID}`
        """
        numero_departement = list(client.query(query_departement).result())

        batch_odisse_canicule_data=[]

        print(f"Lancement de l'ingestion pour les données des décès attribuable à la chaleur ou canicule par département par année depuis l'API Odissé")

        for num_departement in (numero_departement):
            url = "https://odisse.santepubliquefrance.fr/api/explore/v2.1/catalog/datasets/canicules-deces-attribuables-a-la-chaleur-pendant-l-ete-et-pendant-les-vagues-de-chaleur-departement/records"
            code_dep = num_departement["Numero_Departement"]
            # Les paramètres exacts demandés
            params = {
                "lang": "fr",
                "limit": 100,
                "offset": 0,
                "where": f'departements="{code_dep}"'
            }

            while True:
                response = requests.get(url, params=params)
                try:
                    data = response.json()
                except ValueError:
                    data = {}

                if response.status_code == 200 and not data.get("error"):
                    consecutive_rate_limits = 0

                    for item in data.get("results",[]):
                        row={
                            "region": item.get("reglib"),
                            "region_Id": item.get("reg"),
                            "departement": item.get("libgeo"),
                            "numero_departement": item.get("departements"),
                            "annee": item.get("annee"),
                            "classe_age": item.get("classe_age"),
                            "deces_chaleur": item.get("dc_chaleur"),
                            "deces_chaleur_inf": item.get("dc_chaleur_inf"),
                            "deces_chaleur_sup": item.get("dc_chaleur_sup"),
                            "fraction_deces_chaleur": item.get("af_chaleur"),
                            "fraction_deces_chaleur_inf": item.get("af_chaleur_inf"),
                            "fraction_deces_chaleur_sup": item.get("af_chaleur_sup"),
                            "deces_canicule": item.get("dc_canicule"),
                            "deces_canicule_inf": item.get("dc_canicule_inf"),
                            "deces_canicule_sup": item.get("dc_canicule_sup"),
                            "fraction_deces_canicule": item.get("af_canicule"),
                            "fraction_deces_canicule_inf": item.get("af_canicule_inf"),
                            "fraction_deces_canicule_sup": item.get("af_canicule_sup")
                        }

                        batch_odisse_canicule_data.append(row)

                    print(f"-> OK pour département {code_dep}")
                    break

                elif data.get("error") and "limit" in data.get("reason", "").lower():
                    consecutive_rate_limits += 1
                    if consecutive_rate_limits >= 6:
                        charger_dans_bigquery(batch_odisse_canicule_data)
                        print("⚠️ Seuil de 6 limites atteint : sauvegarde d'urgence effectuée.")
                        return
                    print(f"🚦 Limite API atteinte. Pause de 61s... ({consecutive_rate_limits}/6)")
                    time.sleep(61)
                else:
                    print(f"❌ Erreur API ou pas de données pour departement {num_departement}, passage.")
                    break

            if len(batch_odisse_canicule_data) >= 1000:
                charger_dans_bigquery(batch_odisse_canicule_data, DEST_TABLE_ODISSE_CANICULE_DECES_ATTRIBUABLE_CONCERNE_DEPARTEMENT)
                batch_odisse_canicule_data = []

            time.sleep(3)
        
        charger_dans_bigquery(batch_odisse_canicule_data, DEST_TABLE_ODISSE_CANICULE_DECES_ATTRIBUABLE_CONCERNE_DEPARTEMENT)
        print("✅ Pipeline incrémental exécuté avec succès !")

    except Exception as e:
        error_detail = traceback.format_exc()
        print(f"🔥 ERREUR CRITIQUE : {error_detail}")
        raise

if __name__ == "__main__":
    ingest_data_open_meteo()
    #ingest_data_odisse_canicule_deces()
    #ingest_data_odisse_canicule_nb_jour()
    #ingest_data_odisse_canicule_taille_population()
    #ingest_data_odisse_canicule_severite()
    #ingest_data_odisse_canicule_deces_attribuable_CharleurCanicule()