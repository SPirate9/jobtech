import os
import pandas as pd
from datetime import datetime
from loguru import logger
from pymongo import MongoClient
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import re
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
    logger.info(f"Lignes initiales : {len(df)}")

    # Mapping pays
    country_mapping = {
        "fr": "France",
        "de": "Germany",
        "nl": "Netherlands",
        "es": "Spain",
        "it": "Italy",
        "at": "Austria",
        "be": "Belgium",
        "ch": "Switzerland",
        "pl": "Poland",
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

    df = df[(df["salary_min"].isna()) | (df["salary_min"] >= 10000)]
    df = df[(df["salary_max"].isna()) | (df["salary_max"] >= 10000)]

    if df["salary_min"].notna().sum() > 0:
        median_min = df["salary_min"].median()
        df["salary_min"] = df["salary_min"].fillna(median_min)

    if df["salary_max"].notna().sum() > 0:
        median_max = df["salary_max"].median()
        df["salary_max"] = df["salary_max"].fillna(median_max)

    df["salary_avg"] = (df["salary_min"] + df["salary_max"]) / 2

    # Dates
    df["created"] = pd.to_datetime(df["created"], errors="coerce")
    df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce")

    cat_cols = [
        "country", "query", "title", "company", "location",
        "description", "country_name", "skills"
    ]
    cat_cols = [col for col in cat_cols if col in df.columns]

    for col in cat_cols:
        df = df[~df[col].isna()]
        df = df[df[col] != ""]
        df = df[df[col].astype(str) != "[]"]
        df = df[df[col].astype(str) != "['']"]

    logger.info(f"Lignes après nettoyage complet : {len(df)}")

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

    # Chargement depuis MongoDB
    df = pd.DataFrame(list(db["stackoverflow_survey_2024"].find()))
    df.drop(columns=["_id"], inplace=True, errors="ignore")

    useful_cols = [
        "Country", "LanguageHaveWorkedWith", "CompTotal", "Currency",
        "DevType", "YearsCodePro", "Employment", "EdLevel"
    ]
    df = df[[col for col in useful_cols if col in df.columns]]

    # Filtrage Europe uniquement
    eu_countries = ["Germany", "France", "Netherlands", "Spain", "Italy", "Poland", "Switzerland", "Austria", "Belgium"]
    df = df[df["Country"].isin(eu_countries)]

    # Nettoyage CompTotal
    if "CompTotal" in df.columns:
        df["CompTotal"] = pd.to_numeric(df["CompTotal"], errors="coerce")
        df = df[df["CompTotal"].notna()]
        df = df[(df["CompTotal"] > 10000) & (df["CompTotal"] < 500000)]

    # Liste des colonnes catégorielles à filtrer
    cat_cols = ["Country", "LanguageHaveWorkedWith", "Currency", "DevType", "YearsCodePro", "Employment", "EdLevel"]
    cat_cols = [col for col in cat_cols if col in df.columns]  # filtrer celles qui existent réellement

    # Supprimer lignes avec valeurs nulles ou vides ou listes vides dans les colonnes catégorielles
    for col in cat_cols:
        df = df[~df[col].isna()]                         # enlever les nulls
        df = df[df[col] != ""]                           # enlever les chaînes vides
        df = df[df[col].astype(str) != "[]"]             # enlever les listes vides comme chaînes
        df = df[df[col].astype(str) != "['']"]           # enlever les listes avec chaîne vide

    # Créer la colonne "languages_list"
    if "LanguageHaveWorkedWith" in df.columns:
        df["languages_list"] = df["LanguageHaveWorkedWith"].fillna("").str.split(";")

    # Sauvegarde
    df.to_csv(f"{CLEAN_DATA_DIR}/stackoverflow_survey_clean.csv", index=False, encoding="utf-8")
    logger.info(f"Stack Overflow nettoyé: {len(df)} réponses")


def clean_indeed_jobs():
    logger.info("Nettoyage des données Indeed")
    df = pd.DataFrame(list(db["indeed_jobs"].find()))
    df.drop(columns=["_id"], inplace=True, errors="ignore")
    logger.info(f"Lignes initiales : {len(df)}")
    
    # Garde seulement les offres avec titre et entreprise
    df = df[df["title"].notna() & (df["title"] != "")]
    df = df[df["company"].notna() & (df["company"] != "")]
    df = df.drop_duplicates(subset=["id"], keep="first")
    
    # Extraire infos utiles de la description avant de la supprimer
    if "description" in df.columns:
        # Extraction de salaires depuis description
        for idx, row in df.iterrows():
            desc = str(row.get("description", "")).lower()
            
            # Chercher patterns de salaires
            import re
            salary_patterns = [
                r'(\d+)[k€]\s*-\s*(\d+)[k€]',  # 50k€ - 70k€
                r'€\s*(\d+)[,.]?(\d+)?\s*k?\s*-\s*€?\s*(\d+)[,.]?(\d+)?\s*k?',  # €50k - €70k
                r'salary:\s*€?(\d+)[,.]?(\d+)?\s*k?',  # salary: 50k
            ]
            
            for pattern in salary_patterns:
                match = re.search(pattern, desc)
                if match and pd.isna(row.get("min_amount")):
                    try:
                        min_sal = float(match.group(1))
                        max_sal = float(match.group(2)) if len(match.groups()) > 1 else min_sal
                        
                        # Si en k, multiplier par 1000
                        if 'k' in match.group(0):
                            min_sal *= 1000
                            max_sal *= 1000
                            
                        df.at[idx, "min_amount"] = min_sal
                        df.at[idx, "max_amount"] = max_sal
                        break
                    except:
                        pass
        
        # SUPPRIMER la colonne description (inutile maintenant)
        df = df.drop(columns=["description"], errors="ignore")
    
    # Extraction skills depuis titre
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
        if "angular" in title_lower:
            skills.append("Angular")
        if "node" in title_lower:
            skills.append("Node.js")
        return skills
    
    df["skills"] = df["title"].apply(extract_skills)
    
    # Salaires : garde NA si pas de données
    df["min_amount"] = pd.to_numeric(df["min_amount"], errors="coerce")
    df["max_amount"] = pd.to_numeric(df["max_amount"], errors="coerce")
    df["salary_avg"] = (df["min_amount"] + df["max_amount"]) / 2
    
    # Dates
    df["date_posted"] = pd.to_datetime(df["date_posted"], errors="coerce")
    df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce")
    
    # Suppression des colonnes inutiles (URLs, logos, etc.)
    columns_to_remove = [
        "job_url", "job_url_direct", "company_url_direct", "url", "redirect_url", 
        "company_url", "company_logo", "logo", "logo_url", "company_logo_url", 
        "apply_url", "external_url", "thumbnail", "image", "company_image", "favicon"
    ]
    df = df.drop(columns=[col for col in columns_to_remove if col in df.columns], errors='ignore')
    
    logger.info(f"Lignes après nettoyage : {len(df)}")
    df.to_csv(f"{CLEAN_DATA_DIR}/indeed_jobs_clean.csv", index=False, encoding="utf-8")
    logger.info(f"Indeed nettoyé: {len(df)} offres")


def clean_linkedin_jobs():
    logger.info("Nettoyage des données LinkedIn")
    df = pd.DataFrame(list(db["linkedin_jobs"].find()))
    df.drop(columns=["_id"], inplace=True, errors="ignore")
    logger.info(f"Lignes initiales : {len(df)}")
    
    # Garde seulement les offres avec titre et entreprise
    df = df[df["title"].notna() & (df["title"] != "")]
    df = df[df["company"].notna() & (df["company"] != "")]
    df = df.drop_duplicates(subset=["id"], keep="first")
    
    # Extraire infos utiles de la description avant de la supprimer
    if "description" in df.columns:
        # Extraction de salaires depuis description
        for idx, row in df.iterrows():
            desc = str(row.get("description", "")).lower()
            
            # Chercher patterns de salaires
            import re
            salary_patterns = [
                r'(\d+)[k€$]\s*-\s*(\d+)[k€$]',  # 50k€ - 70k€
                r'[€$]\s*(\d+)[,.]?(\d+)?\s*k?\s*-\s*[€$]?\s*(\d+)[,.]?(\d+)?\s*k?',  # €50k - €70k
                r'salary:\s*[€$]?(\d+)[,.]?(\d+)?\s*k?',  # salary: 50k
                r'compensation:\s*[€$]?(\d+)[,.]?(\d+)?\s*k?',  # compensation: 50k
            ]
            
            for pattern in salary_patterns:
                match = re.search(pattern, desc)
                if match and pd.isna(row.get("min_amount")):
                    try:
                        min_sal = float(match.group(1))
                        max_sal = float(match.group(2)) if len(match.groups()) > 1 else min_sal
                        
                        # Si en k, multiplier par 1000
                        if 'k' in match.group(0):
                            min_sal *= 1000
                            max_sal *= 1000
                            
                        df.at[idx, "min_amount"] = min_sal
                        df.at[idx, "max_amount"] = max_sal
                        break
                    except:
                        pass
        
        # SUPPRIMER la colonne description (inutile maintenant)
        df = df.drop(columns=["description"], errors="ignore")
    
    # Extraction skills depuis titre
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
        if "angular" in title_lower:
            skills.append("Angular")
        if "node" in title_lower:
            skills.append("Node.js")
        if "full stack" in title_lower:
            skills.append("Full Stack")
        return skills
    
    df["skills"] = df["title"].apply(extract_skills)
    
    # Salaires : garde NA si pas de données (LinkedIn en a rarement)
    df["min_amount"] = pd.to_numeric(df["min_amount"], errors="coerce")
    df["max_amount"] = pd.to_numeric(df["max_amount"], errors="coerce")
    df["salary_avg"] = (df["min_amount"] + df["max_amount"]) / 2
    
    # Dates
    df["date_posted"] = pd.to_datetime(df["date_posted"], errors="coerce")
    df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce")
    
    # Suppression des colonnes inutiles (URLs, logos, etc.)
    columns_to_remove = [
        "job_url", "job_url_direct", "company_url_direct", "url", "redirect_url", 
        "company_url", "company_logo", "logo", "logo_url", "company_logo_url", 
        "apply_url", "external_url", "thumbnail", "image", "company_image", "favicon"
    ]
    df = df.drop(columns=[col for col in columns_to_remove if col in df.columns], errors='ignore')
    
    logger.info(f"Lignes après nettoyage : {len(df)}")
    df.to_csv(f"{CLEAN_DATA_DIR}/linkedin_jobs_clean.csv", index=False, encoding="utf-8")
    logger.info(f"LinkedIn nettoyé: {len(df)} offres")

def create_dimension_tables():
    logger.info("Création des tables de dimensions")

    countries_data = [
        {"iso2": "FR", "country_name": "France", "region": "Western Europe", "currency": "EUR"},
        {"iso2": "DE", "country_name": "Germany", "region": "Western Europe", "currency": "EUR"},
        {"iso2": "NL", "country_name": "Netherlands", "region": "Western Europe", "currency": "EUR"},
        {"iso2": "ES", "country_name": "Spain", "region": "Southern Europe", "currency": "EUR"},
        {"iso2": "IT", "country_name": "Italy", "region": "Southern Europe", "currency": "EUR"},
        {"iso2": "AT", "country_name": "Austria", "region": "Central Europe", "currency": "EUR"},
        {"iso2": "BE", "country_name": "Belgium", "region": "Western Europe", "currency": "EUR"},
        {"iso2": "CH", "country_name": "Switzerland", "region": "Western Europe", "currency": "EUR"},
        {"iso2": "PL", "country_name": "Poland", "region": "Eastern Europe", "currency": "EUR"}
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
        {"source_name": "Indeed"},
        {"source_name": "LinkedIn"}
    ]
    pd.DataFrame(sources_data).to_csv(f"{CLEAN_DATA_DIR}/dim_sources.csv", index=False)

    logger.info("Tables de dimensions créées")

def clean_html_simple(text):
    """Nettoie le HTML très simplement"""
    if pd.isna(text) or text == "":
        return ""
    try:
        # Retirer toutes les balises HTML
        soup = BeautifulSoup(str(text), 'html.parser')
        clean_text = soup.get_text()
        # Nettoyer les espaces
        clean_text = re.sub(r'\s+', ' ', clean_text).strip()
        return clean_text
    except:
        return ""

def main():
    start = datetime.now()
    logger.info("Démarrage du nettoyage TalentInsight")

    clean_adzuna_jobs()
    clean_github_trends()
    clean_google_trends()
    clean_stackoverflow_survey()
    clean_indeed_jobs()
    clean_linkedin_jobs()
    create_dimension_tables()

    duration = datetime.now() - start
    logger.info(f"Nettoyage terminé en {duration.total_seconds():.1f}s")

    for file in os.listdir(CLEAN_DATA_DIR):
        if file.endswith(".csv"):
            size = os.path.getsize(os.path.join(CLEAN_DATA_DIR, file))
            logger.info(f"Fichier nettoyé: {file} ({size} bytes)")


if __name__ == "__main__":
    main()