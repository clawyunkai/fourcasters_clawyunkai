import os
import time
import pandas as pd
import requests

# 1. PARAMÈTRES

fichier_poi = "Departement_V3_centroide.csv"
fichier_sortie = "meteo_toutes_communes.csv"
fichier_suivi = "communes_terminees.csv"

date_debut = "2000-01-01"
date_fin = "2026-08-01"

pause = 10

# 2. VARIABLES MÉTÉO-

variables_meteo = [
    "weather_code",
    "temperature_2m_mean",
    "temperature_2m_min",
    "temperature_2m_max",
    "apparent_temperature_mean",
    "apparent_temperature_min",
    "apparent_temperature_max",
    "relative_humidity_2m_mean",
    "relative_humidity_2m_min",
    "relative_humidity_2m_max",
    "dew_point_2m_mean",
    "precipitation_sum",
    "rain_sum",
    "snowfall_sum",
    "precipitation_hours",
    "wind_speed_10m_mean",
    "wind_speed_10m_max",
    "wind_gusts_10m_max",
    "wind_direction_10m_dominant",
    "cloud_cover_mean",
    "pressure_msl_mean",
    "sunshine_duration",
    "shortwave_radiation_sum",
    "et0_fao_evapotranspiration",
    "vapour_pressure_deficit_max",
    "soil_moisture_0_to_7cm_mean",
    "soil_moisture_7_to_28cm_mean",
    "soil_moisture_28_to_100cm_mean",
    "soil_temperature_0_to_7cm_mean"
]

# 3. REQUÊTE POUR UNE COMMUNE

def get_weather_by_poi(nom_poi, numero_departement, latitude, longitude, start_date, end_date):
    url = "https://archive-api.open-meteo.com/v1/archive"

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "daily": ",".join(variables_meteo),
        "timezone": "Europe/Paris",
        "models": "era5_seamless"
    }

    try:
        response = requests.get(url, params=params, timeout=300)

        if response.status_code == 429:
            print("🚦 - Limite atteinte. Pause de 60 secondes...")
            time.sleep(60)
            response = requests.get(url, params=params, timeout=300)

        response.raise_for_status()
        data = response.json()

        if "daily" not in data:
            return None

        df = pd.DataFrame(data["daily"])
        df["nom_poi"] = nom_poi
        df["numero_departement"] = numero_departement
        df["latitude_poi"] = latitude
        df["longitude_poi"] = longitude

        return df

    except requests.RequestException as erreur:
        print(f"❌ - Erreur pour {nom_poi} ({numero_departement}) : {erreur}")
        return None


# 4. LECTURE DU FICHIER DES COMMUNES

df_pois = pd.read_csv(fichier_poi, sep=None, engine="python", encoding="utf-8-sig", dtype={"Numero_Departement": "string"})

df_pois.columns = df_pois.columns.str.strip()

df_pois["Latitude"] = pd.to_numeric(df_pois["Latitude"], errors="coerce")
df_pois["Longitude"] = pd.to_numeric(df_pois["Longitude"], errors="coerce")

df_pois = df_pois.dropna(subset=["Commune", "Latitude", "Longitude"]).reset_index(drop=True)

df_pois["Numero_Departement"] = df_pois["Numero_Departement"].astype(str).str.replace(".0", "", regex=False).str.zfill(2)


# 5. COMMUNES DÉJÀ TERMINÉES

if os.path.exists(fichier_suivi):
    df_suivi = pd.read_csv(fichier_suivi, dtype={"numero_departement": "string"})
    communes_terminees = set(zip(df_suivi["nom_poi"], df_suivi["numero_departement"]))
else:
    communes_terminees = set()

premiere_ecriture = not os.path.exists(fichier_sortie)

print(f"📍 - {len(df_pois)} communes dans le fichier")
print(f"✅ - {len(communes_terminees)} communes déjà terminées")
print(f"📅 - Période : {date_debut} → {date_fin}")


# 6. BOUCLE SUR LES COMMUNES

for index, commune in df_pois.iterrows():
    nom_poi = commune["Commune"]
    numero_departement = commune["Numero_Departement"]
    latitude = commune["Latitude"]
    longitude = commune["Longitude"]

    cle_commune = (nom_poi, numero_departement)

    if cle_commune in communes_terminees:
        print(f"⏭️ - [{index + 1}/{len(df_pois)}] {nom_poi} ({numero_departement}) déjà terminée")
        continue

    print(f"🌦️ - [{index + 1}/{len(df_pois)}] Requête pour {nom_poi} ({numero_departement})...")

    df_commune = get_weather_by_poi(nom_poi, numero_departement, latitude, longitude, date_debut, date_fin)

    if df_commune is not None and not df_commune.empty:
        df_commune.to_csv(fichier_sortie, mode="a", header=premiere_ecriture, index=False, encoding="utf-8-sig")

        premiere_ecriture = False

        nouvelle_commune = pd.DataFrame([{"nom_poi": nom_poi, "numero_departement": numero_departement}])
        nouvelle_commune.to_csv(fichier_suivi, mode="a", header=not os.path.exists(fichier_suivi), index=False, encoding="utf-8-sig")

        communes_terminees.add(cle_commune)

        print(f"✅ - {nom_poi} ({numero_departement}) : {len(df_commune):,} jours récupérés")
    else:
        print(f"⚠️ - Aucune donnée enregistrée pour {nom_poi} ({numero_departement})")

    print(f"😴 - Pause de {pause} secondes...\n")
    time.sleep(pause)


print("🏁 - Collecte terminée !")
print(f"💾 - Données : {fichier_sortie}")
print(f"📋 - Suivi : {fichier_suivi}")