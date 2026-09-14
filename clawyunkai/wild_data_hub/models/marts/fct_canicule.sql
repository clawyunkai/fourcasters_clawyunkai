WITH canicule_nb_jours AS (
    SELECT * FROM {{ source('donnees_brutes', 'odisse_canicule_nb_jours_departement') }}
),

canicule_population AS (
    SELECT * FROM {{ source('donnees_brutes', 'odisse_canicule_population_concerne_departement') }}
),

canicule_severite AS (
    SELECT * FROM {{ source('donnees_brutes', 'odisse_canicule_severite_departement') }}
)

SELECT
    cnj.region,
    cnj.departement,
    cnj.numero_departement,
    cnj.annee,
    cnj.nb_jour_canicules,
    cp.population_exposee,
    cs.severite
FROM canicule_nb_jours cnj
FULL OUTER JOIN canicule_population cp 
    ON cp.annee=cnj.annee 
    AND cp.numero_departement=cnj.numero_departement
FULL OUTER JOIN canicule_severite cs
    ON cs.annee=cnj.annee 
    AND cs.numero_departement=cnj.numero_departement

