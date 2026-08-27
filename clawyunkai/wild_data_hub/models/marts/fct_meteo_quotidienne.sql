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

SELECT *
FROM fusion_totale
-- 🛡️ SÉCURITÉ : Déduplication métier. 
-- Si on a deux fois la même ville à la même date, on garde la plus récente.
QUALIFY ROW_NUMBER() OVER(PARTITION BY code_insee, date ORDER BY insere_a DESC) = 1