import os
import json
import pandas as pd
import hashlib
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

uri = os.getenv("MONGO_URI")
client = MongoClient(uri, serverSelectionTimeoutMS=5000)
db = client['jobtech']

# Connexion test
try:
    client.server_info()
    print("[LOG] Connexion MongoDB réussie.")
except Exception as e:
    print("[ERREUR] Connexion MongoDB échouée :", e)
    exit(1)

# Répertoire des fichiers
RAW_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'jobtech', 'raw'))
print(f"[LOG] Recherche dans : {RAW_DIR}")

def get_files(raw_dir):
    for root, _, files in os.walk(raw_dir):
        for file in files:
            if file.endswith('.csv') or file.endswith('.json'):
                yield os.path.join(root, file)

def hash_record(record):
    """Crée un ID unique à partir du contenu du document"""
    return hashlib.md5(json.dumps(record, sort_keys=True).encode('utf-8')).hexdigest()

def load_data(filepath):
    print(f"[LOG] Chargement : {filepath}")
    try:
        if filepath.endswith('.csv'):
            df = pd.read_csv(filepath).dropna(how='all')
            data = df.fillna("").to_dict(orient='records')
        elif filepath.endswith('.json'):
            with open(filepath, encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, dict):
                    data = [data]
        else:
            data = []
        print(f"[LOG] {len(data)} lignes chargées.")
        return data
    except Exception as e:
        print(f"[ERREUR] Chargement échoué : {e}")
        return []

def get_collection_name(filepath):
    return os.path.splitext(os.path.basename(filepath))[0]

def insert_data(collection_name, data):
    if not data:
        return
    try:
        for doc in data:
            doc['_id'] = hash_record(doc)
        result = db[collection_name].insert_many(data, ordered=False)
        print(f"[LOG] {len(result.inserted_ids)} documents insérés dans '{collection_name}'.")
    except Exception as e:
        print(f"[ERREUR] Insertion dans '{collection_name}' : {e}")

def main():
    files = list(get_files(RAW_DIR))
    if not files:
        print(f"[LOG] Aucun fichier .csv/.json trouvé dans {RAW_DIR}")
        return

    for filepath in files:
        collection_name = get_collection_name(filepath)
        data = load_data(filepath)
        insert_data(collection_name, data)

if __name__ == "__main__":
    main()
