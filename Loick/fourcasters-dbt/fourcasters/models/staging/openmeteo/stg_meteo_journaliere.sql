{{ config(materialized = 'view') }}

-- Lecture de la table brute BigQuery
WITH donnees_source AS (

    SELECT *
    FROM {{ source('openmeteo_raw', 'meteo_journaliere') }}

)

SELECT
    -- Métadonnées techniques
    row_hash,
    insere_a,

    -- Fusion des colonnes géographiques en double
    COALESCE(
        NULLIF(TRIM(nom_poi), ''),
        NULLIF(TRIM(Ville), '')
    ) AS ville,

    -- Conservation temporaire du numéro de département pour la future jointure avec le référentiel
    CASE
        WHEN UPPER(TRIM(numero_departement)) IN ('2A', '2B')
            THEN UPPER(TRIM(numero_departement))

        ELSE LPAD(
            COALESCE(
                CAST(SAFE_CAST(NULLIF(TRIM(numero_departement), '') AS INT64) AS STRING),
                CAST(SAFE_CAST(Departement AS INT64) AS STRING)),2,'0')
    END AS numero_departement,

    COALESCE(
        latitude_poi,
        Latitude
    ) AS latitude,

    COALESCE(
        longitude_poi,
        Longitude
    ) AS longitude,

    -- Date et code météorologique
    time AS date,
    SAFE_CAST(weather_code AS INT64) AS code_meteo,

    -- Températures
    temperature_2m_mean
        AS temperature_moyenne,

    temperature_2m_min
        AS temperature_minimale,

    temperature_2m_max
        AS temperature_maximale,

    apparent_temperature_mean
        AS temperature_ressentie_moyenne,

    apparent_temperature_min
        AS temperature_ressentie_minimale,

    apparent_temperature_max
        AS temperature_ressentie_maximale,

    -- Humidité et point de rosée
    relative_humidity_2m_mean
        AS humidite_moyenne,

    relative_humidity_2m_min
        AS humidite_minimale,

    relative_humidity_2m_max
        AS humidite_maximale,

    dew_point_2m_mean
        AS point_de_rosee_moyen,

    -- Précipitations
    precipitation_sum
        AS precipitations_totales,

    rain_sum
        AS pluie_totale,

    snowfall_sum
        AS neige_totale,

    precipitation_hours
        AS heures_de_precipitations,

    -- Vent
    wind_speed_10m_mean
        AS vitesse_vent_moyenne,

    wind_speed_10m_max
        AS vitesse_vent_maximale,

    wind_gusts_10m_max
        AS rafale_vent_maximale,

    wind_direction_10m_dominant
        AS direction_vent_dominante,

    -- Atmosphère et ensoleillement
    cloud_cover_mean
        AS couverture_nuageuse_moyenne,

    pressure_msl_mean
        AS pression_moyenne,

    sunshine_duration
        AS duree_ensoleillement,

    shortwave_radiation_sum
        AS rayonnement_solaire_total,

    -- Évapotranspiration et pression de vapeur
    et0_fao_evapotranspiration
        AS evapotranspiration,

    vapour_pressure_deficit_max
        AS deficit_pression_vapeur_maximal,

    -- Humidité du sol
    soil_moisture_0_to_7cm_mean
        AS humidite_sol_0_7cm,

    soil_moisture_7_to_28cm_mean
        AS humidite_sol_7_28cm,

    soil_moisture_28_to_100cm_mean
        AS humidite_sol_28_100cm,

    -- Température du sol
    soil_temperature_0_to_7cm_mean
        AS temperature_sol_0_7cm

FROM donnees_source