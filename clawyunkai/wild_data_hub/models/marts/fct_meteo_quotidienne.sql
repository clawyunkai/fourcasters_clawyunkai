WITH historique AS (
    -- On appelle la vue historique nettoyée
    SELECT * FROM {{ ref('stg_historique_meteo') }}
),

quotidien AS (
    -- On appelle la vue incrémentale nettoyée
    SELECT * FROM {{ ref('stg_open_meteo') }}
),

fusion_totale AS (
    -- UNION ALL empile les deux tables de manière ultra-rapide
    SELECT * FROM historique
    UNION ALL
    SELECT * FROM quotidien
)

SELECT
    -- 1. Clés (Métadonnées et Clés Étrangères)
    row_hash,
    insere_a,
    code_insee,   -- Clé vers dim_communes
    ville,
    CAST(date AS DATE) AS date, -- Clé vers dim_date
    code_meteo,   -- Clé vers dim_code_meteo

    -- 2. Mesures : Températures
    temperature_moyenne,
    temperature_minimale,
    temperature_maximale,
    temperature_ressentie_moyenne,
    temperature_ressentie_minimale,
    temperature_ressentie_maximale,
    
    -- 3. Mesures : Humidité et Précipitations
    humidite_moyenne,
    humidite_minimale,
    humidite_maximale,
    point_de_rosee_moyen,
    precipitations_totales,
    pluie_totale,
    neige_totale,
    heures_de_precipitations,
    
    -- 4. Mesures : Vent et Pression
    vitesse_vent_moyenne,
    vitesse_vent_maximale,
    rafale_vent_maximale,
    direction_vent_dominante,
    couverture_nuageuse_moyenne,
    pression_moyenne, -- Correction de la coquille
    
    -- 5. Mesures : Soleil et Sol
    duree_ensoleillement,
    rayonnement_solaire_total,
    evapotranspiration,
    deficit_pression_vapeur_maximal,
    humidite_sol_0_7cm,
    humidite_sol_7_28cm,
    humidite_sol_28_100cm,
    temperature_sol_0_7cm
FROM fusion_totale
QUALIFY ROW_NUMBER() OVER(PARTITION BY code_insee, date ORDER BY insere_a DESC) = 1