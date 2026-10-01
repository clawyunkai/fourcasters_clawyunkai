WITH meteo AS (
    SELECT
        SUBSTRING(code_insee, 1, 2) AS code_departement,
        CAST(date AS DATE) AS date,
        EXTRACT(MONTH FROM date) AS mois,
        CASE
            WHEN EXTRACT(MONTH FROM date) <= 5 THEN EXTRACT(YEAR FROM date)
            WHEN EXTRACT(MONTH FROM date) >= 9 THEN EXTRACT(YEAR FROM date) + 1
        END AS annee_reference_canicule,
        temperature_moyenne,
        temperature_maximale,
        precipitations_totales,
        humidite_moyenne
    FROM {{ ref('fct_meteo_quotidienne') }}
),

meteo_saisons AS (
    SELECT
        code_departement,
        annee_reference_canicule,

        -- Automne (sept-nov de l'année précédente)
        AVG(IF(mois IN (9, 10, 11), temperature_moyenne, NULL))     AS t_moy_automne,
        AVG(IF(mois IN (9, 10, 11), precipitations_totales, NULL))  AS precip_automne,

        -- Hiver (déc-fév) : recharge des nappes et des sols
        AVG(IF(mois IN (12, 1, 2), temperature_moyenne, NULL))      AS t_moy_hiver,
        AVG(IF(mois IN (12, 1, 2), precipitations_totales, NULL))   AS precip_hiver,

        -- Printemps (mars-mai) : un printemps sec favorise les étés chauds
        AVG(IF(mois IN (3, 4, 5), temperature_moyenne, NULL))       AS t_moy_printemps,
        AVG(IF(mois IN (3, 4, 5), precipitations_totales, NULL))    AS precip_printemps,
        AVG(IF(mois IN (3, 4, 5), humidite_moyenne, NULL))          AS humidite_printemps,
        AVG(IF(mois IN (3, 4, 5),
            IF(precipitations_totales < 1, 1, 0), NULL))         AS part_jours_secs_printemps,

        -- Mai seul : le mois le plus proche de l'été
        AVG(IF(mois = 5, temperature_maximale, NULL))               AS t_max_mai,
        AVG(IF(mois = 5,
            IF(temperature_maximale >= 25, 1, 0), NULL))         AS part_jours_chauds_mai

    FROM meteo
    WHERE annee_reference_canicule >= 2004   -- les lignes NULL (juin-août) sont exclues ici
    GROUP BY code_departement, annee_reference_canicule
)

SELECT
    ms.*,
    fc.nb_jour_canicules,
    CASE
        WHEN fc.nb_jour_canicules >0 THEN TRUE
        ELSE FALSE
    END AS evenement_caniculaire,
    fc.population_exposee,
    fc.severite
FROM meteo_saisons AS ms
LEFT JOIN {{ ref('fct_canicule') }} AS fc
    ON  fc.numero_departement = ms.code_departement
    AND fc.annee = ms.annee_reference_canicule