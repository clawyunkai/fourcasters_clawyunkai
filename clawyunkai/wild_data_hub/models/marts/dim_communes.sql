WITH referentiel_geo AS (
    SELECT * FROM {{ source('referentiels', 'departements_latitude_longitude') }}
)

SELECT DISTINCT
    `code INSEE` AS code_insee,
    `Région` AS region,
    Departement AS departement,
    Numero_Departement AS numero_departement,
    Commune as commune,
    Latitude AS latitude,
    Longitude AS longitude
FROM referentiel_geo