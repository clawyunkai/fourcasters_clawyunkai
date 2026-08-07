import os
import pandas as pd
from google.cloud import bigquery

# 1. Authentification (indique le chemin vers ton fichier JSON)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "fourcasters_cle.json"

# 2. Initialiser le client BigQuery
client = bigquery.Client()

# 3. Écrire la requête SQL pour récupérer toute la table
# Attention aux backticks (`) autour du nom de la table, c'est obligatoire en SQL BigQuery
query = """
    SELECT * 
    FROM `les-fourcasters.imports_csv.departements_latitude_longitude`
"""

print("Récupération des données en cours...")

# 4. Lancer la requête et la transformer directement en DataFrame
df = client.query(query).to_dataframe()

# 5. Afficher le résultat dans le terminal de VS Code
print("Voici un aperçu du DataFrame :")
print(df.head()) # Affiche les 5 premières lignes