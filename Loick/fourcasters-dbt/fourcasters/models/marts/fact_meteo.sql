{{ config(
    materialized = 'table',

    partition_by = {
        "field": "date",
        "data_type": "date",
        "granularity": "month"
    },

    cluster_by = ["code_insee"]
) }}

SELECT

    -- Identifiant unique de l'observation
    row_hash AS id_observation_meteo,

    -- Clés vers les dimensions
    DATE(date) AS date,
    code_insee,

    -- Code météo
    code_meteo,

    -- Températures
    temperature_moyenne,
    temperature_minimale,
    temperature_maximale,

    temperature_ressentie_moyenne,
    temperature_ressentie_minimale,
    temperature_ressentie_maximale,

    -- Humidité
    humidite_moyenne,
    humidite_minimale,
    humidite_maximale,

    point_de_rosee_moyen,

    -- Précipitations
    precipitations_totales,
    pluie_totale,
    neige_totale,
    heures_de_precipitations,

    -- Vent
    vitesse_vent_moyenne,
    vitesse_vent_maximale,
    rafale_vent_maximale,
    direction_vent_dominante,

    -- Atmosphère
    couverture_nuageuse_moyenne,
    pression_moyenne,

    duree_ensoleillement,
    rayonnement_solaire_total,

    -- Évapotranspiration
    evapotranspiration,
    deficit_pression_vapeur_maximal,

    -- Sol
    humidite_sol_0_7cm,
    humidite_sol_7_28cm,
    humidite_sol_28_100cm,

    temperature_sol_0_7cm

FROM {{ ref('int_meteo_communes') }}