import os
from dotenv import load_dotenv
from google.cloud import bigquery

# Charge le fichier .env (utile si GOOGLE_APPLICATION_CREDENTIALS y est défini)
load_dotenv()

# Option la plus directe : cibler explicitement la clé JSON
# (Ajuste le chemin vers "fourcasters_cle.json" si le fichier est dans un sous-dossier)
chemin_cle = "fourcasters_cle.json" 

if os.path.exists(chemin_cle):
    client = bigquery.Client.from_service_account_json(chemin_cle)
else:
    # Si la variable d'environnement est déjà chargée dans le terminal/.env
    client = bigquery.Client(project="les-fourcasters")

# Nom exact du dataset à créer dans ton projet BigQuery
dataset_id = f"{client.project}.machine_learning"

dataset = bigquery.Dataset(dataset_id)
dataset.location = "us-central1"

# Création (ne plante pas s'il existe déjà)
dataset = client.create_dataset(dataset, exists_ok=True)

print(f"Dataset « {dataset.dataset_id} » configuré avec succès dans la région {dataset.location}.")