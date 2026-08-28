# Projet dbt Fourcasters

Ce dossier contient le projet dbt chargé de transformer les données météorologiques Open-Meteo stockées dans Google BigQuery.

La documentation générale du pipeline se trouve dans le fichier [`../README.md`](../README.md).

## Sources

Le projet utilise la source BigQuery suivante :

```text
fourcasters-openmeteo-loick.openmeteo_raw.meteo_journaliere
```

Elle est déclarée dans :

```text
models/staging/openmeteo/_openmeteo_sources.yml
```

## Seed

Le référentiel géographique des 360 points d’observation est chargé par dbt depuis :

```text
seeds/referentiel_communes.csv
```

Les types, descriptions et tests du seed sont déclarés dans :

```text
seeds/_seeds.yml
```

## Organisation des modèles

```text
models/
├── staging/
│   └── openmeteo/
│       └── stg_meteo_journaliere.sql
├── intermediate/
│   └── int_meteo_communes.sql
└── marts/
    ├── dim_commune.sql
    ├── dim_date.sql
    └── fact_meteo.sql
```

## Matérialisations

- `staging` : vues ;
- `intermediate` : vues ;
- `marts` : tables ;
- `fact_meteo` : table partitionnée par mois et organisée par code INSEE.

## Commandes principales

Les commandes doivent être exécutées depuis le dossier parent `fourcasters-dbt`.

### Vérifier la configuration

```bash
uv run dbt debug --project-dir fourcasters
```

### Charger uniquement le seed

```bash
uv run dbt seed --project-dir fourcasters
```

### Construire les modèles

```bash
uv run dbt run --project-dir fourcasters
```

### Exécuter les tests

```bash
uv run dbt test --project-dir fourcasters
```

### Construire et tester l’ensemble du projet

```bash
uv run dbt build --project-dir fourcasters
```

### Générer la documentation

```bash
uv run dbt docs generate --project-dir fourcasters
uv run dbt docs serve --project-dir fourcasters
```

## Résultat attendu

Un `dbt build` complet doit construire :

- `stg_meteo_journaliere` ;
- `referentiel_communes` ;
- `int_meteo_communes` ;
- `dim_commune` ;
- `dim_date` ;
- `fact_meteo`.

Tous les tests dbt doivent terminer avec le statut `PASS`.