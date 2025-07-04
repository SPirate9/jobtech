#!/usr/bin/env python3
"""
01_scrape.py - Collecte de données multi-sources pour TalentInsight (sans Selenium)
"""

import os
import json
import requests
import pandas as pd
from datetime import datetime
from pytrends.request import TrendReq
import time
import random
from loguru import logger
from dotenv import load_dotenv
import zipfile
from jobspy import scrape_jobs

load_dotenv()

RAW_DATA_DIR = "raw"
os.makedirs(RAW_DATA_DIR, exist_ok=True)

logger.remove()
logger.add(f"{RAW_DATA_DIR}/scraping.log", level="INFO", rotation="1 day")


def scrape_google_trends():
    """Collecte les tendances Google avec une initialisation Pytrends corrigée."""
    logger.info("Démarrage Google Trends")
    trends_data = []

    try:
        pytrends = TrendReq(timeout=(10, 25))
        keywords = ["Python", "JavaScript", "React", "Go", "Rust", "Data Science"]

        for keyword in keywords:
            try:
                logger.info(f"Collecte de la tendance pour : '{keyword}'")
                pytrends.build_payload(
                    [keyword], cat=0, timeframe="today 3-m", geo="", gprop=""
                )
                interest_over_time_df = pytrends.interest_over_time()

                if (
                    not interest_over_time_df.empty
                    and not interest_over_time_df[keyword].empty
                ):
                    interest = interest_over_time_df[keyword].to_dict()
                    trends_data.append(
                        {
                            "source": "google_trends",
                            "keyword": keyword,
                            "interest_over_time": {
                                str(k.date()): v for k, v in interest.items()
                            },
                            "scraped_at": datetime.now().isoformat(),
                        }
                    )
                    logger.info(f"Tendance pour '{keyword}' collectée.")
                else:
                    logger.warning(f"Aucune donnée de tendance pour '{keyword}'.")

                time.sleep(random.randint(20, 35))

            except Exception as e:
                logger.error(f"Erreur Pytrends pour '{keyword}': {e}")
                if "429" in str(e):
                    logger.error("Rate limit atteint. Pause de 120s.")
                    time.sleep(120)
                continue

    except Exception as e:
        logger.error(f"Erreur critique Google Trends: {e}")

    if trends_data:
        with open(f"{RAW_DATA_DIR}/google_trends.json", "w", encoding="utf-8") as f:
            json.dump(trends_data, f, indent=2, ensure_ascii=False)

    logger.info(f"Google Trends: {len(trends_data)} tendances collectées.")


def scrape_adzuna_api():
    """Collecte via API Adzuna"""
    logger.info("Démarrage API Adzuna")

    app_id = os.getenv("ADZUNA_APP_ID")
    api_key = os.getenv("ADZUNA_API_KEY")

    if not app_id or not api_key:
        logger.warning("Clés Adzuna manquantes dans .env")
        return

    countries = ["fr", "de", "nl", "es", "it", "pl", "gb", "ch", "at", "be"]
    tech_queries = ["python developer", "javascript developer", "react developer"]
    adzuna_data = []

    for country in countries:
        for query in tech_queries:
            try:
                url = f"https://api.adzuna.com/v1/api/jobs/{country}/search/1"
                params = {
                    "app_id": app_id,
                    "app_key": api_key,
                    "what": query,
                    "results_per_page": 50,
                    "content-type": "application/json",
                }

                response = requests.get(url, params=params, timeout=30)
                if response.status_code == 200:
                    data = response.json()

                    for job in data.get("results", []):
                        job_data = {
                            "source": "adzuna",
                            "country": country,
                            "query": query,
                            "title": job.get("title"),
                            "company": job.get("company", {}).get("display_name"),
                            "location": job.get("location", {}).get("display_name"),
                            "salary_min": job.get("salary_min"),
                            "salary_max": job.get("salary_max"),
                            "description": job.get("description"),
                            "created": job.get("created"),
                            "scraped_at": datetime.now().isoformat(),
                        }
                        adzuna_data.append(job_data)

                time.sleep(1)

            except Exception as e:
                logger.error(f"Erreur Adzuna {country}/{query}: {e}")
                continue

    with open(f"{RAW_DATA_DIR}/adzuna_jobs.json", "w", encoding="utf-8") as f:
        json.dump(adzuna_data, f, indent=2, ensure_ascii=False)

    logger.info(f"Adzuna terminé: {len(adzuna_data)} offres collectées")


def scrape_github_trends():
    """Collecte GitHub trending repos"""
    logger.info("Démarrage GitHub API")

    github_token = os.getenv("GITHUB_TOKEN")
    headers = {"Authorization": f"token {github_token}"} if github_token else {}

    languages = ["Rust","Python", "JavaScript", "Go", "TypeScript", "Java"]
    github_data = []

    for lang in languages:
        try:
            url = "https://api.github.com/search/repositories"
            params = {
                "q": f"language:{lang} created:>2024-01-01",
                "sort": "stars",
                "order": "desc",
                "per_page": 100,
            }

            response = requests.get(url, params=params, headers=headers, timeout=30)
            if response.status_code == 200:
                data = response.json()

                for repo in data.get("items", []):
                    repo_data = {
                        "source": "github",
                        "language": lang,
                        "name": repo.get("name"),
                        "full_name": repo.get("full_name"),
                        "owner_location": repo.get("owner", {}).get("location"),
                        "stars": repo.get("stargazers_count"),
                        "forks": repo.get("forks_count"),
                        "created_at": repo.get("created_at"),
                        "updated_at": repo.get("updated_at"),
                        "description": repo.get("description"),
                        "scraped_at": datetime.now().isoformat(),
                    }
                    github_data.append(repo_data)

            time.sleep(1)

        except Exception as e:
            logger.error(f"Erreur GitHub {lang}: {e}")
            continue

    with open(f"{RAW_DATA_DIR}/github_trends.json", "w", encoding="utf-8") as f:
        json.dump(github_data, f, indent=2, ensure_ascii=False)

    logger.info(f"GitHub terminé: {len(github_data)} repos collectés")


def download_stackoverflow_survey():
    """Télécharge Stack Overflow Survey 2024 depuis le site officiel (ZIP)."""
    logger.info("Démarrage Stack Overflow Survey")
    zip_file_path = f"{RAW_DATA_DIR}/stackoverflow_survey_2024.zip"
    survey_file_path = f"{RAW_DATA_DIR}/stackoverflow_survey_2024.csv"

    try:
        zip_url = "https://survey.stackoverflow.co/datasets/stack-overflow-developer-survey-2024.zip"
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"
        }

        response = requests.get(zip_url, headers=headers, timeout=180, stream=True)
        response.raise_for_status()

        with open(zip_file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
            csv_files = [
                f
                for f in zip_ref.namelist()
                if f.endswith(".csv") and "survey_results_public" in f.lower()
            ]
            if csv_files:
                csv_filename = csv_files[0]
                zip_ref.extract(csv_filename, RAW_DATA_DIR)
                extracted_path = os.path.join(RAW_DATA_DIR, csv_filename)
                os.rename(extracted_path, survey_file_path)
            else:
                logger.error("Aucun fichier CSV trouvé dans le ZIP")
                return

        os.remove(zip_file_path)

        logger.info(
            f"Stack Overflow: {os.path.getsize(survey_file_path)} bytes téléchargés"
        )

    except Exception as e:
        logger.error(f"Erreur Stack Overflow: {e}")


def scrape_indeed_linkedin_jobs():
    """Collecte des offres d'emploi Indeed + LinkedIn"""
    logger.info("Démarrage collecte Indeed + LinkedIn Europe")
    
    countries_config = {
        'France': {'country': 'France', 'location': 'Paris, France'},
        'Germany': {'country': 'Germany', 'location': 'Berlin, Germany'},
        'Italy': {'country': 'Italy', 'location': 'Milan, Italy'},
        'Spain': {'country': 'Spain', 'location': 'Madrid, Spain'},
        'Netherlands': {'country': 'Netherlands', 'location': 'Amsterdam, Netherlands'},
        'Belgium': {'country': 'Belgium', 'location': 'Brussels, Belgium'},
        'Switzerland': {'country': 'Switzerland', 'location': 'Zurich, Switzerland'},
        'Austria': {'country': 'Austria', 'location': 'Vienna, Austria'},
        'Poland': {'country': 'Poland', 'location': 'Warsaw, Poland'}
    }
    
    tech_keywords = [
        '"software engineer" python',
        '"data scientist" machine learning',
        '"devops engineer" cloud',
        '"full stack developer" javascript',
        '"backend developer" API',
        '"frontend developer" react'
    ]
    
    all_jobs_data = []
    
    for country_name, config in countries_config.items():
        logger.info(f"Collecte Indeed + LinkedIn {country_name}")
        
        for keyword in tech_keywords:
            try:
                jobs = scrape_jobs(
                    site_name=["indeed", "linkedin"],
                    search_term=keyword,
                    location=config['location'],
                    country_indeed=config['country'],
                    results_wanted=25,
                    hours_old=168,
                    job_type='fulltime',
                    description_format='html',
                    linkedin_fetch_description=True,
                    verbose=1
                )
                
                if not jobs.empty:
                    jobs['search_keyword'] = keyword
                    jobs['target_country'] = country_name
                    jobs['scraped_at'] = pd.Timestamp.now()
                    all_jobs_data.append(jobs)
                    
                    indeed_count = len(jobs[jobs['site'] == 'indeed'])
                    linkedin_count = len(jobs[jobs['site'] == 'linkedin'])
                    logger.info(f"{country_name}/{keyword}: Indeed={indeed_count}, LinkedIn={linkedin_count}")
                
                time.sleep(5)
                    
            except Exception as e:
                logger.error(f"Erreur {country_name}/{keyword}: {e}")
                continue
    
    if all_jobs_data:
        all_jobs = pd.concat(all_jobs_data, ignore_index=True)
        all_jobs = all_jobs.drop_duplicates(subset=['title', 'company', 'location', 'site'])
        indeed_jobs = all_jobs[all_jobs['site'] == 'indeed']
        linkedin_jobs = all_jobs[all_jobs['site'] == 'linkedin']
        
        if not indeed_jobs.empty:
            indeed_json = indeed_jobs.to_dict('records')
            with open(f"{RAW_DATA_DIR}/indeed_jobs.json", "w", encoding="utf-8") as f:
                json.dump(indeed_json, f, indent=2, ensure_ascii=False, default=str)
            logger.info(f"Indeed terminé: {len(indeed_jobs)} offres")
        
        if not linkedin_jobs.empty:
            linkedin_json = linkedin_jobs.to_dict('records')
            with open(f"{RAW_DATA_DIR}/linkedin_jobs.json", "w", encoding="utf-8") as f:
                json.dump(linkedin_json, f, indent=2, ensure_ascii=False, default=str)
            logger.info(f"LinkedIn terminé: {len(linkedin_jobs)} offres")
        
        logger.info(f"Total collecté: {len(all_jobs)} offres (Indeed + LinkedIn)")
    else:
        logger.warning("Aucune donnée Indeed/LinkedIn collectée")


def main():
    """Fonction principale"""
    start_time = datetime.now()

    scrape_adzuna_api()
    scrape_github_trends()
    scrape_google_trends()
    download_stackoverflow_survey()
    scrape_indeed_linkedin_jobs()

    end_time = datetime.now()
    duration = end_time - start_time

    logger.info(f"Collecte terminée en {duration.total_seconds():.1f}s")

    raw_files = [f for f in os.listdir(RAW_DATA_DIR) if f.endswith((".json", ".csv"))]
    for file in raw_files:
        file_path = os.path.join(RAW_DATA_DIR, file)
        size = os.path.getsize(file_path)
        logger.info(f"Fichier: {file} ({size} bytes)")


if __name__ == "__main__":
    main()
