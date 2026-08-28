WITH meteo AS (

    SELECT *
    FROM {{ ref('stg_meteo_journaliere') }}

),

communes AS (

    SELECT *
    FROM {{ ref('referentiel_communes') }}

)

SELECT

    -- Identifiants techniques
    m.row_hash,
    m.insere_a,

    -- Date
    m.date,

    -- Référentiel géographique officiel
    c.code_insee,
    c.commune,
    c.numero_departement,
    c.departement,
    c.region,
    c.service,
    c.centroide,

    -- Coordonnées du point météo
    m.latitude,
    m.longitude,

    -- Code météo
    m.code_meteo,

    -- Températures
    m.temperature_moyenne,
    m.temperature_minimale,
    m.temperature_maximale,
    m.temperature_ressentie_moyenne,
    m.temperature_ressentie_minimale,
    m.temperature_ressentie_maximale,

    -- Humidité
    m.humidite_moyenne,
    m.humidite_minimale,
    m.humidite_maximale,
    m.point_de_rosee_moyen,

    -- Précipitations
    m.precipitations_totales,
    m.pluie_totale,
    m.neige_totale,
    m.heures_de_precipitations,

    -- Vent
    m.vitesse_vent_moyenne,
    m.vitesse_vent_maximale,
    m.rafale_vent_maximale,
    m.direction_vent_dominante,

    -- Atmosphère
    m.couverture_nuageuse_moyenne,
    m.pression_moyenne,
    m.duree_ensoleillement,
    m.rayonnement_solaire_total,

    -- Évapotranspiration
    m.evapotranspiration,
    m.deficit_pression_vapeur_maximal,

    -- Sol
    m.humidite_sol_0_7cm,
    m.humidite_sol_7_28cm,
    m.humidite_sol_28_100cm,
    m.temperature_sol_0_7cm

FROM meteo AS m

INNER JOIN communes AS c

    ON LOWER(TRIM(m.ville)) = LOWER(TRIM(c.commune))
    AND m.numero_departement = c.numero_departement