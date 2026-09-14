WITH canicule_deces_attribuable AS (
    SELECT * FROM {{ source('donnees_brutes', 'odisse_canicule_deces_attribuable_departement') }}
)

SELECT DISTINCT
    *
FROM canicule_deces_attribuable