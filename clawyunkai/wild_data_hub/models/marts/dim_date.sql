WITH tableau_dates AS (
    -- Génération de toutes les dates entre le 01/01/2000 et le 01/01/2028
    SELECT date_jour
    FROM UNNEST(GENERATE_DATE_ARRAY('2000-01-01', '2028-01-01')) AS date_jour
)

SELECT
    -- 1. La clé principale (qui fera le lien avec fct_meteo_quotidienne)
    date_jour AS date,

    -- 2. Les éléments numériques de base
    EXTRACT(YEAR FROM date_jour) AS annee,
    EXTRACT(MONTH FROM date_jour) AS mois,
    EXTRACT(DAY FROM date_jour) AS jour,
    EXTRACT(QUARTER FROM date_jour) AS trimestre,
    EXTRACT(DAYOFYEAR FROM date_jour) AS jour_annee,

    -- 3. Noms des jours de la semaine (BigQuery donne des numéros de 1(Dimanche) à 7(Samedi))
    CASE EXTRACT(DAYOFWEEK FROM date_jour)
        WHEN 1 THEN 'Dimanche'
        WHEN 2 THEN 'Lundi'
        WHEN 3 THEN 'Mardi'
        WHEN 4 THEN 'Mercredi'
        WHEN 5 THEN 'Jeudi'
        WHEN 6 THEN 'Vendredi'
        WHEN 7 THEN 'Samedi'
    END AS nom_jour,

    -- 4. Noms des mois
    CASE EXTRACT(MONTH FROM date_jour)
        WHEN 1 THEN 'Janvier'   WHEN 2 THEN 'Février'
        WHEN 3 THEN 'Mars'      WHEN 4 THEN 'Avril'
        WHEN 5 THEN 'Mai'       WHEN 6 THEN 'Juin'
        WHEN 7 THEN 'Juillet'   WHEN 8 THEN 'Août'
        WHEN 9 THEN 'Septembre' WHEN 10 THEN 'Octobre'
        WHEN 11 THEN 'Novembre' WHEN 12 THEN 'Décembre'
    END AS nom_mois,

    -- 5. Indicateur binaire très pratique pour l'analyse
    CASE 
        WHEN EXTRACT(DAYOFWEEK FROM date_jour) IN (1, 7) THEN TRUE 
        ELSE FALSE 
    END AS est_weekend,

    -- 6. Les saisons météorologiques
    CASE 
        WHEN EXTRACT(MONTH FROM date_jour) IN (3, 4, 5) THEN 'Printemps'
        WHEN EXTRACT(MONTH FROM date_jour) IN (6, 7, 8) THEN 'Été'
        WHEN EXTRACT(MONTH FROM date_jour) IN (9, 10, 11) THEN 'Automne'
        WHEN EXTRACT(MONTH FROM date_jour) IN (12, 1, 2) THEN 'Hiver'
    END AS saison

FROM tableau_dates