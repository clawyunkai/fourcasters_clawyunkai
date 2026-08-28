# Fourcasters — Pipeline météo Open-Meteo avec dbt

## Présentation

Ce dépôt contient le volet dbt du projet **Fourcasters**, réalisé dans le cadre de la formation Data Analyst de la Wild Code School.

Fourcasters vise à exploiter des données météorologiques historiques afin d’étudier les conditions climatiques en France et de préparer des analyses liées aux risques tels que les canicules, les précipitations extrêmes et les feux de forêt.

Cette partie du projet transforme les données Open-Meteo stockées dans BigQuery en un modèle analytique en étoile, exploitable dans Power BI et pour les futurs travaux de Machine Learning.

## Périmètre actuel

- Source principale : API Open-Meteo Historical
- Modèle météorologique : `ERA5-Seamless`
- Granularité : une observation quotidienne par point géographique
- Période historique : du 1er janvier 2000 au 31 juillet 2026
- Zone étudiée : France métropolitaine
- Référentiel géographique : 360 points d’observation
- Entrepôt de données : Google BigQuery
- Outil de transformation : dbt

## Architecture des données

```text
Open-Meteo Historical API
        │
        ▼
Collecte et préparation Python
        │
        ▼
BigQuery — openmeteo_landing
└── historique_openmeteo
        │
        ▼
BigQuery — openmeteo_raw
└── meteo_journaliere
        │
        ▼
dbt — openmeteo_analyse
├── stg_meteo_journaliere
├── referentiel_communes
├── int_meteo_communes
├── dim_commune
├── dim_date
└── fact_meteo
```

## Modèle analytique

Le projet produit un modèle en étoile composé de deux dimensions et d’une table de faits.

### `dim_commune`

Dimension géographique contenant les 360 communes ou points d’observation :

- code INSEE ;
- nom de la commune ;
- département ;
- région ;
- latitude et longitude ;
- type de service administratif ;
- indicateur de centroïde.

### `dim_date`

Dimension calendaire couvrant toute la période météorologique :

- date ;
- année ;
- trimestre ;
- mois ;
- semaine ;
- jour de la semaine ;
- saison ;
- indicateur de week-end.

### `fact_meteo`

Table de faits contenant les observations météorologiques quotidiennes :

- températures ;
- températures ressenties ;
- humidité ;
- point de rosée ;
- précipitations, pluie et neige ;
- vitesse et direction du vent ;
- couverture nuageuse ;
- pression atmosphérique ;
- ensoleillement ;
- rayonnement solaire ;
- évapotranspiration ;
- déficit de pression de vapeur ;
- humidité et température du sol.

La table est partitionnée par mois sur la colonne `date` et organisée par `code_insee`.

## Organisation du projet

```text
fourcasters-dbt/
├── fourcasters/
│   ├── analyses/
│   ├── macros/
│   ├── models/
│   │   ├── staging/
│   │   │   └── openmeteo/
│   │   ├── intermediate/
│   │   └── marts/
│   ├── seeds/
│   │   └── referentiel_communes.csv
│   ├── snapshots/
│   ├── tests/
│   └── dbt_project.yml
├── src/
├── .gitignore
├── .python-version
├── pyproject.toml
└── uv.lock
```

## Modèles dbt

### Staging

`stg_meteo_journaliere` :

- lit la table BigQuery `openmeteo_raw.meteo_journaliere` ;
- harmonise les colonnes géographiques ;
- normalise les codes de département ;
- renomme les variables météorologiques ;
- convertit les colonnes dans les types attendus.

### Intermediate

`int_meteo_communes` :

- joint les observations météorologiques au référentiel des communes ;
- ajoute le code INSEE, le département et la région ;
- prépare les données utilisées par les tables analytiques.

### Marts

Les marts contiennent :

- `dim_commune` ;
- `dim_date` ;
- `fact_meteo`.

## Qualité des données

Des tests dbt sont définis pour vérifier :

- l’absence de valeurs nulles sur les colonnes essentielles ;
- l’unicité des identifiants ;
- l’unicité des codes INSEE ;
- l’unicité des dates ;
- l’intégrité des relations entre la table de faits et les dimensions.

Le dernier `dbt build` validé a produit :

- 360 lignes dans `referentiel_communes` ;
- 360 lignes dans `dim_commune` ;
- environ 9 700 lignes dans `dim_date` ;
- environ 3,5 millions de lignes dans `fact_meteo` ;
- 100 % des tests dbt réussis.

## Prérequis

- Python 3.12 ou une version compatible ;
- `uv` ;
- un accès au projet Google Cloud `fourcasters-openmeteo-loick` ;
- une clé de compte de service BigQuery conservée en dehors du dépôt ;
- un fichier `profiles.yml` configuré dans le dossier personnel `.dbt`.

Les clés GCP, les identifiants et le fichier `profiles.yml` ne doivent jamais être ajoutés à Git.

## Installation

Depuis le dossier `Loick/fourcasters-dbt` :

```bash
uv sync
```

## Vérifier la connexion à BigQuery

```bash
uv run dbt debug --project-dir fourcasters
```

## Construire le pipeline dbt

```bash
uv run dbt build --project-dir fourcasters
```

Cette commande :

1. charge le seed des 360 communes ;
2. construit les modèles de staging ;
3. construit le modèle intermédiaire ;
4. construit les dimensions et la table de faits ;
5. exécute les tests de qualité.

## Générer la documentation dbt

```bash
uv run dbt docs generate --project-dir fourcasters
```

Puis :

```bash
uv run dbt docs serve --project-dir fourcasters
```

## Sécurité

Les éléments suivants sont exclus du versionnement :

- `.venv/` ;
- `target/` ;
- `logs/` ;
- `dbt_packages/` ;
- `.env` ;
- `profiles.yml` ;
- les clés de comptes de service GCP.

## Auteur

**MARTIN Loïck**
Projet de fin de formation Data Analyst — Wild Code School