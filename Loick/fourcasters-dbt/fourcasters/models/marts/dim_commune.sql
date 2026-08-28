SELECT

    code_insee,
    commune,

    numero_departement,
    departement,
    region,

    latitude,
    longitude,

    service,
    centroide

FROM {{ ref('referentiel_communes') }}