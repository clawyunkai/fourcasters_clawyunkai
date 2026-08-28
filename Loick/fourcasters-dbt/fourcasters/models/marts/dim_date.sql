WITH limites AS (

    SELECT
        MIN(DATE(date)) AS date_min,
        MAX(DATE(date)) AS date_max

    FROM {{ ref('int_meteo_communes') }}

),

calendrier AS (

    SELECT date

    FROM limites,

    UNNEST(
        GENERATE_DATE_ARRAY(
            date_min,
            date_max
        )
    ) AS date

)

SELECT

    date,

    EXTRACT(YEAR FROM date) AS annee,

    EXTRACT(QUARTER FROM date) AS trimestre,

    EXTRACT(MONTH FROM date) AS numero_mois,

    CASE EXTRACT(MONTH FROM date)

        WHEN 1 THEN 'Janvier'
        WHEN 2 THEN 'Février'
        WHEN 3 THEN 'Mars'
        WHEN 4 THEN 'Avril'
        WHEN 5 THEN 'Mai'
        WHEN 6 THEN 'Juin'
        WHEN 7 THEN 'Juillet'
        WHEN 8 THEN 'Août'
        WHEN 9 THEN 'Septembre'
        WHEN 10 THEN 'Octobre'
        WHEN 11 THEN 'Novembre'
        WHEN 12 THEN 'Décembre'

    END AS mois,

    EXTRACT(WEEK FROM date) AS numero_semaine,

    EXTRACT(DAY FROM date) AS jour_du_mois,

    EXTRACT(DAYOFWEEK FROM date) AS numero_jour_semaine,

    CASE EXTRACT(DAYOFWEEK FROM date)

        WHEN 1 THEN 'Dimanche'
        WHEN 2 THEN 'Lundi'
        WHEN 3 THEN 'Mardi'
        WHEN 4 THEN 'Mercredi'
        WHEN 5 THEN 'Jeudi'
        WHEN 6 THEN 'Vendredi'
        WHEN 7 THEN 'Samedi'

    END AS jour_semaine,

    CASE

        WHEN EXTRACT(MONTH FROM date) IN (12, 1, 2)
            THEN 'Hiver'

        WHEN EXTRACT(MONTH FROM date) IN (3, 4, 5)
            THEN 'Printemps'

        WHEN EXTRACT(MONTH FROM date) IN (6, 7, 8)
            THEN 'Été'

        WHEN EXTRACT(MONTH FROM date) IN (9, 10, 11)
            THEN 'Automne'

    END AS saison,

    CASE
        WHEN EXTRACT(DAYOFWEEK FROM date) IN (1, 7)
            THEN TRUE
        ELSE FALSE
    END AS est_weekend

FROM calendrier