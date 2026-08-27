WITH source AS (
    SELECT * FROM {{ source('donnees_brutes', 'open_meteo_20000101_to_20260731') }}
    WHERE time <= '2026-07-31'
),

referentiel_geo AS (
    SELECT * FROM {{ source('referentiels', 'departements_latitude_longitude') }}
),

joined_data AS (
    SELECT 
        s.*,
        r.`Région` AS ref_region,
        r.Departement AS ref_departement,
        r.Latitude AS ref_latitude,
        r.Longitude AS ref_longitude,
        r.`CODE INSEE` AS ref_code_insee
    FROM source AS s
    LEFT JOIN referentiel_geo AS r
        ON COALESCE(s.nom_poi, s.Ville) = r.Commune
        AND LPAD(COALESCE(s.numero_departement, CAST(s.Departement AS STRING)), 2, '0') = r.Numero_Departement
)

SELECT
    CAST(TO_HEX(MD5(CONCAT(COALESCE(Ville, nom_poi), '_', time))) AS STRING) AS row_hash,
    CURRENT_TIMESTAMP() AS insere_a,
    COALESCE(Ville, nom_poi) AS ville,
    CAST(ref_code_insee AS STRING) AS code_insee,
    CAST(COALESCE(numero_departement, CAST(Departement AS STRING), ref_departement) AS STRING) AS departement,
    CAST(ref_region AS STRING) AS region,
    COALESCE(Latitude, latitude_poi, ref_latitude) AS latitude,
    COALESCE(Longitude, longitude_poi, ref_longitude) AS longitude,
    CAST(time AS DATE) AS date,
    CAST(weather_code AS INT64) AS code_meteo,
    temperature_2m_mean AS temperature_moyenne,
    temperature_2m_min AS temperature_minimale,
    temperature_2m_max AS temperature_maximale,
    apparent_temperature_mean AS temperature_ressentie_moyenne,
    apparent_temperature_min AS temperature_ressentie_minimale,
    apparent_temperature_max AS temperature_ressentie_maximale,
    relative_humidity_2m_mean AS humidite_moyenne,
    relative_humidity_2m_min AS humidite_minimale,
    relative_humidity_2m_max AS humidite_maximale,
    dew_point_2m_mean AS point_de_rosee_moyen,
    precipitation_sum AS precipitations_totales,
    rain_sum AS pluie_totale,
    snowfall_sum AS neige_totale,
    precipitation_hours AS heures_de_precipitations,
    wind_speed_10m_mean AS vitesse_vent_moyenne,
    wind_speed_10m_max AS vitesse_vent_maximale,
    wind_gusts_10m_max AS rafale_vent_maximale,
    wind_direction_10m_dominant AS direction_vent_dominante,
    cloud_cover_mean AS couverture_nuageuse_moyenne,
    pressure_msl_mean AS pression_moyenne,
    sunshine_duration AS duree_ensoleillement,
    shortwave_radiation_sum AS rayonnement_solaire_total,
    et0_fao_evapotranspiration AS evapotranspiration,
    vapour_pressure_deficit_max AS deficit_pression_vapeur_maximal,
    soil_moisture_0_to_7cm_mean AS humidite_sol_0_7cm,
    soil_moisture_7_to_28cm_mean AS humidite_sol_7_28cm,
    soil_moisture_28_to_100cm_mean AS humidite_sol_28_100cm,
    soil_temperature_0_to_7cm_mean AS temperature_sol_0_7cm
FROM joined_data