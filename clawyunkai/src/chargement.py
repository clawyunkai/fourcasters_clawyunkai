## Fonctions pour la requête et l'ingestion des données des API
import os
import time
import json
import hashlib
import traceback
from datetime import datetime, timezone, timedelta, date
from google.cloud import bigquery
from dotenv import load_dotenv
import logging

logger=logging.getLogger(__name__)

load_dotenv()

client = bigquery.Client()

def calculer_hash(row: dict) -> str:
    """Génère un hash unique pour la ligne, indépendant de l'ordre des colonnes."""
    row_str = json.dumps(row, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(row_str.encode()).hexdigest()

def enrichir_lignes(data: list) -> list:
    """Ajoute le timestamp d'insertion et le hash à chaque ligne."""
    now = datetime.now(timezone.utc).isoformat()
    enriched = []
    for row in data:
        row_copy = dict(row)
        row_copy["row_hash"] = calculer_hash(row)
        row_copy["inserted_at"] = now
        enriched.append(row_copy)
    return enriched

def charger_une_page(donnees: list, batch_id: str, table_id: str) -> int:
    """Insere une page de donnees dans la table brute. Renvoie le nombre de lignes."""
    maintenant = datetime.now(timezone.utc).isoformat()

    lignes = []
    for ligne in donnees:
        enrichie = dict(ligne)
        enrichie["row_hash"] = calculer_hash(ligne)   # hash AVANT les colonnes de suivi
        enrichie["inserted_at"] = maintenant
        enrichie["batch_id"] = batch_id
        lignes.append(enrichie)

    config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
    client.load_table_from_json(lignes, table_id, job_config=config).result()

    print(f"{len(lignes)} lignes inserees (lot {batch_id})")
    return len(lignes)

def annuler_le_lot(batch_id: str, table_id: str) -> None:
    """Supprime toutes les lignes inserees par cette execution."""
    requete = f"DELETE FROM `{table_id}` WHERE batch_id = @batch_id"
    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("batch_id", "STRING", batch_id)
        ]
    )
    client.query(requete, job_config=config).result()
    print(f"Lot {batch_id} supprime")

def charger_dans_bigquery(data: list, dest_table_id: str):
    """Charge un lot de données dans BigQuery."""
    if not data:
        return
    data_enrichie = enrichir_lignes(data)
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
    )
    load_job = client.load_table_from_json(data_enrichie, dest_table_id, job_config=job_config)
    load_job.result()