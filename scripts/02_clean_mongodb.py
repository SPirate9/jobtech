import os
import pandas as pd
from datetime import datetime
from loguru import logger
from pymongo import MongoClient
from dotenv import load_dotenv
load_dotenv()
# Répertoires
CLEAN_DATA_DIR = "datasets_clean"
os.makedirs(CLEAN_DATA_DIR, exist_ok=True)

# Logger
logger.remove()
logger.add(f"{CLEAN_DATA_DIR}/cleaning.log", level="INFO", rotation="1 day")

# Connexion MongoDB via variable d'environnement
load_dotenv()
uri = os.getenv("MONGO_URI")
if not uri:
    logger.error("La variable d'environnement MONGO_URI est manquante.")
    exit(1)
try:
    client = MongoClient(uri)
    db = client["jobtech"]
    client.server_info()
    logger.info("Connexion MongoDB réussie.")
except Exception as e:
    logger.error(f"Échec de connexion à MongoDB : {e}")
    exit(1)

def clean_adzuna_jobs():
    logger.info("Nettoyage des données Adzuna")
    df = pd.DataFrame(list(db["adzuna_jobs"].find()))
    df.drop(columns=["_id"], inplace=True, errors="ignore")

    country_mapping = {
        "fr": "France", "de": "Germany", "nl": "Netherlands",
        "es": "Spain", "it": "Italy"
    }
    df["country_name"] = df["country"].map(country_mapping)

    def extract_skills(title):
        if not title:
            return []
        title_lower = title.lower()
        skills = []
        if "python" in title_lower:
            skills.append("Python")
        if "javascript" in title_lower or "js" in title_lower:
            skills.append("JavaScript")
        if "react" in title_lower:
            skills.append("React")
        if "java" in title_lower and "javascript" not in title_lower:
            skills.append("Java")
        if "node" in title_lower:
            skills.append("Node.js")
        return skills

    df["skills"] = df["title"].apply(extract_skills)

    df["salary_min"] = pd.to_numeric(df["salary_min"], errors="coerce")
    df["salary_max"] = pd.to_numeric(df["salary_max"], errors="coerce")
    df["salary_avg"] = (df["salary_min"] + df["salary_max"]) / 2

    df["created"] = pd.to_datetime(df["created"], errors="coerce")
    df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce")

    df.to_csv(f"{CLEAN_DATA_DIR}/adzuna_jobs_clean.csv", index=False, encoding="utf-8")
    logger.info(f"Adzuna nettoyé: {len(df)} offres")


def clean_github_trends():
    logger.info("Nettoyage des données GitHub")
    df = pd.DataFrame(list(db["github_trends"].find()))
    df.drop(columns=["_id"], inplace=True, errors="ignore")

    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")
    df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce")
    df["stars"] = pd.to_numeric(df["stars"], errors="coerce").fillna(0)
    df["forks"] = pd.to_numeric(df["forks"], errors="coerce").fillna(0)
    df["popularity_score"] = df["stars"] * 0.7 + df["forks"] * 0.3

    df.to_csv(f"{CLEAN_DATA_DIR}/github_trends_clean.csv", index=False, encoding="utf-8")
    logger.info(f"GitHub nettoyé: {len(df)} repos")


def clean_google_trends():
    logger.info("Nettoyage des données Google Trends")
    data = list(db["google_trends"].find())

    trends_list = []
    for item in data:
        keyword = item.get("keyword")
        for date_str, value in item.get("interest_over_time", {}).items():
            trends_list.append({
                "keyword": keyword,
                "date": date_str,
                "interest_value": value,
                "scraped_at": item.get("scraped_at"),
            })

    df = pd.DataFrame(trends_list)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce")
    df["interest_value"] = pd.to_numeric(df["interest_value"], errors="coerce")

    df.to_csv(f"{CLEAN_DATA_DIR}/google_trends_clean.csv", index=False, encoding="utf-8")
    logger.info(f"Google Trends nettoyé: {len(df)} points de données")


def clean_stackoverflow_survey():
    logger.info("Nettoyage des données Stack Overflow")
    df = pd.DataFrame(list(db["stackoverflow_survey_2024"].find()))
    df.drop(columns=["_id"], inplace=True, errors="ignore")

    useful_cols = [
        "Country", "LanguageHaveWorkedWith", "CompTotal", "Currency",
        "DevType", "YearsCodePro", "Employment", "EdLevel"
    ]
    df = df[[col for col in useful_cols if col in df.columns]]

    eu_countries = ["Germany", "France", "Netherlands", "Spain", "Italy", "Poland"]
    df = df[df["Country"].isin(eu_countries)]

    if "CompTotal" in df.columns:
        df["CompTotal"] = pd.to_numeric(df["CompTotal"], errors="coerce")
        df = df[df["CompTotal"].notna()]
        df = df[(df["CompTotal"] > 10000) & (df["CompTotal"] < 500000)]

    if "LanguageHaveWorkedWith" in df.columns:
        df["languages_list"] = df["LanguageHaveWorkedWith"].fillna("").str.split(";")

    df.to_csv(f"{CLEAN_DATA_DIR}/stackoverflow_survey_clean.csv", index=False, encoding="utf-8")
    logger.info(f"Stack Overflow nettoyé: {len(df)} réponses")


def create_dimension_tables():
    logger.info("Création des tables de dimensions")

    countries_data = [
        {"iso2": "FR", "country_name": "France", "region": "Western Europe", "currency": "EUR"},
        {"iso2": "DE", "country_name": "Germany", "region": "Western Europe", "currency": "EUR"},
        {"iso2": "NL", "country_name": "Netherlands", "region": "Western Europe", "currency": "EUR"},
        {"iso2": "ES", "country_name": "Spain", "region": "Southern Europe", "currency": "EUR"},
        {"iso2": "IT", "country_name": "Italy", "region": "Southern Europe", "currency": "EUR"},
    ]
    pd.DataFrame(countries_data).to_csv(f"{CLEAN_DATA_DIR}/dim_countries.csv", index=False)

    skills_data = [
        {"skill_group": "Programming Language", "tech_label": "Python"},
        {"skill_group": "Programming Language", "tech_label": "JavaScript"},
        {"skill_group": "Programming Language", "tech_label": "Java"},
        {"skill_group": "Framework", "tech_label": "React"},
        {"skill_group": "Runtime", "tech_label": "Node.js"},
        {"skill_group": "Programming Language", "tech_label": "Go"},
        {"skill_group": "Programming Language", "tech_label": "Rust"},
        {"skill_group": "Programming Language", "tech_label": "TypeScript"},
        {"skill_group": "Field", "tech_label": "Data Science"},
    ]
    pd.DataFrame(skills_data).to_csv(f"{CLEAN_DATA_DIR}/dim_skills.csv", index=False)

    sources_data = [
        {"source_name": "Adzuna"},
        {"source_name": "GitHub"},
        {"source_name": "Google Trends"},
        {"source_name": "Stack Overflow"},
    ]
    pd.DataFrame(sources_data).to_csv(f"{CLEAN_DATA_DIR}/dim_sources.csv", index=False)

    logger.info("Tables de dimensions créées")


def main():
    start = datetime.now()
    logger.info("Démarrage du nettoyage TalentInsight")

    clean_adzuna_jobs()
    clean_github_trends()
    clean_google_trends()
    clean_stackoverflow_survey()
    create_dimension_tables()

    duration = datetime.now() - start
    logger.info(f"Nettoyage terminé en {duration.total_seconds():.1f}s")

    for file in os.listdir(CLEAN_DATA_DIR):
        if file.endswith(".csv"):
            size = os.path.getsize(os.path.join(CLEAN_DATA_DIR, file))
            logger.info(f"Fichier nettoyé: {file} ({size} bytes)")


if __name__ == "__main__":
    main()
