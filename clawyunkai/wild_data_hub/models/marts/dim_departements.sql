WITH referentiel_geo AS (
    SELECT * FROM {{ source('referentiels', 'departements_latitude_longitude') }}
)

SELECT DISTINCT
    `Région` AS region,
    Departement AS departement,
    Numero_Departement AS numero_departement,
FROM referentiel_geo