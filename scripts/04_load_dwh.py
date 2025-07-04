#!/usr/bin/env python3
"""
03_load_dwh.py - Chargement Data Warehouse TalentInsight
"""

import os
import sqlite3
import pandas as pd
from datetime import datetime
from loguru import logger

CLEAN_DATA_DIR = "datasets_clean"
DWH_DIR = "dwh"
os.makedirs(DWH_DIR, exist_ok=True)

logger.remove()
logger.add(f"{DWH_DIR}/dwh.log", level="INFO", rotation="1 day")


def create_dwh_schema(conn):
    """Crée le schéma du Data Warehouse"""
    logger.info("Création du schéma Data Warehouse")

    # Table de dates
    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS d_date (
        date_key TEXT PRIMARY KEY,
        day INTEGER,
        month INTEGER,
        quarter INTEGER,
        year INTEGER,
        day_week INTEGER
    )
    """
    )

    # Table pays
    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS d_country (
        id_country INTEGER PRIMARY KEY AUTOINCREMENT,
        iso2 TEXT UNIQUE NOT NULL,
        country_name TEXT,
        region TEXT,
        currency TEXT
    )
    """
    )

    # Table compétences
    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS d_skill (
        id_skill INTEGER PRIMARY KEY AUTOINCREMENT,
        skill_group TEXT,
        tech_label TEXT UNIQUE
    )
    """
    )

    # Table sources
    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS d_source (
        id_source INTEGER PRIMARY KEY AUTOINCREMENT,
        source_name TEXT UNIQUE
    )
    """
    )

    # Table entreprises
    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS d_company (
        id_company INTEGER PRIMARY KEY AUTOINCREMENT,
        company_name TEXT,
        sector TEXT
    )
    """
    )

    # Fait: Offres d'emploi
    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS f_job_offers (
        id_offer INTEGER PRIMARY KEY AUTOINCREMENT,
        id_country INTEGER,
        id_skill INTEGER,
        id_source INTEGER,
        id_company INTEGER,
        date_key TEXT,
        title TEXT,
        location TEXT,
        salary_min REAL,
        salary_max REAL,
        salary_avg REAL,
        FOREIGN KEY (id_country) REFERENCES d_country(id_country),
        FOREIGN KEY (id_skill) REFERENCES d_skill(id_skill),
        FOREIGN KEY (id_source) REFERENCES d_source(id_source),
        FOREIGN KEY (id_company) REFERENCES d_company(id_company),
        FOREIGN KEY (date_key) REFERENCES d_date(date_key)
    )
    """
    )

    # Fait: Tendances GitHub
    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS f_github_trends (
        id_trend INTEGER PRIMARY KEY AUTOINCREMENT,
        id_skill INTEGER,
        id_source INTEGER,
        date_key TEXT,
        repo_name TEXT,
        stars INTEGER,
        forks INTEGER,
        popularity_score REAL,
        FOREIGN KEY (id_skill) REFERENCES d_skill(id_skill),
        FOREIGN KEY (id_source) REFERENCES d_source(id_source),
        FOREIGN KEY (date_key) REFERENCES d_date(date_key)
    )
    """
    )

    # Fait: Google Trends
    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS f_search_trends (
        id_search INTEGER PRIMARY KEY AUTOINCREMENT,
        id_skill INTEGER,
        id_source INTEGER,
        date_key TEXT,
        interest_value INTEGER,
        FOREIGN KEY (id_skill) REFERENCES d_skill(id_skill),
        FOREIGN KEY (id_source) REFERENCES d_source(id_source),
        FOREIGN KEY (date_key) REFERENCES d_date(date_key)
    )
    """
    )

    # Fait: Stack Overflow Survey
    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS f_survey_responses (
        id_response INTEGER PRIMARY KEY AUTOINCREMENT,
        id_country INTEGER,
        id_source INTEGER,
        salary REAL,
        years_experience TEXT,
        dev_type TEXT,
        languages_used TEXT,
        FOREIGN KEY (id_country) REFERENCES d_country(id_country),
        FOREIGN KEY (id_source) REFERENCES d_source(id_source)
    )
    """
    )

    conn.commit()
    logger.info("Schéma Data Warehouse créé")


def load_dimensions(conn):
    """Charge les tables de dimensions"""
    logger.info("Chargement des dimensions")

    # Dimension pays
    countries_df = pd.read_csv(f"{CLEAN_DATA_DIR}/dim_countries.csv")
    # Ajouter une colonne id_country
    countries_df.insert(0, "id_country", range(1, len(countries_df) + 1))
    countries_df.to_sql("d_country", conn, if_exists="replace", index=False)

    # Dimension compétences
    skills_df = pd.read_csv(f"{CLEAN_DATA_DIR}/dim_skills.csv")
    skills_df.insert(0, "id_skill", range(1, len(skills_df) + 1))
    skills_df.to_sql("d_skill", conn, if_exists="replace", index=False)

    # Dimension sources
    sources_df = pd.read_csv(f"{CLEAN_DATA_DIR}/dim_sources.csv")
    sources_df.insert(0, "id_source", range(1, len(sources_df) + 1))
    sources_df.to_sql("d_source", conn, if_exists="replace", index=False)

    # Dimension dates (générée)
    dates_data = []
    for year in range(2024, 2026):
        for month in range(1, 13):
            for day in range(1, 32):
                try:
                    date_obj = datetime(year, month, day)
                    dates_data.append(
                        {
                            "date_key": date_obj.strftime("%Y-%m-%d"),
                            "day": day,
                            "month": month,
                            "quarter": (month - 1) // 3 + 1,
                            "year": year,
                            "day_week": date_obj.weekday() + 1,
                        }
                    )
                except ValueError:
                    continue

    dates_df = pd.DataFrame(dates_data)
    dates_df.to_sql("d_date", conn, if_exists="replace", index=False)

    logger.info("Dimensions chargées")


def load_job_offers(conn):
    """Charge les offres d'emploi"""
    logger.info("Chargement des offres d'emploi")

    df = pd.read_csv(f"{CLEAN_DATA_DIR}/adzuna_jobs_clean.csv")

    # Mapping vers les IDs de dimension
    country_map = (
        pd.read_sql("SELECT iso2, id_country FROM d_country", conn)
        .set_index("iso2")["id_country"]
        .to_dict()
    )
    source_map = (
        pd.read_sql("SELECT source_name, id_source FROM d_source", conn)
        .set_index("source_name")["id_source"]
        .to_dict()
    )

    # Traitement par chunks pour optimiser
    job_data = []
    for _, row in df.iterrows():
        # Extraction date
        created_date = pd.to_datetime(row["created"], errors="coerce")
        date_key = (
            created_date.strftime("%Y-%m-%d")
            if pd.notna(created_date)
            else "2024-06-30"
        )

        # Mapping IDs
        country_code = {
            "France": "FR",
            "Germany": "DE",
            "Netherlands": "NL",
            "Spain": "ES",
            "Italy": "IT",
            "Belgium": "BE",
            "Switzerland": "CH",
            "Austria":"AT",
            "Poland": "PL"
        }.get(row["country_name"])
        id_country = country_map.get(country_code, 1)
        id_source = source_map.get("Adzuna", 1)

        # Traitement des compétences
        skills = (
            eval(row["skills"])
            if pd.notna(row["skills"]) and row["skills"] != "[]"
            else ["Unknown"]
        )
        for skill in skills:
            skill_id = conn.execute(
                "SELECT id_skill FROM d_skill WHERE tech_label = ?", (skill,)
            ).fetchone()
            id_skill = skill_id[0] if skill_id else 1

            job_data.append(
                {
                    "id_country": id_country,
                    "id_skill": id_skill,
                    "id_source": id_source,
                    "id_company": 1,
                    "date_key": date_key,
                    "title": row["title"],
                    "location": row["location"],
                    "salary_min": (
                        row["salary_min"] if pd.notna(row["salary_min"]) else None
                    ),
                    "salary_max": (
                        row["salary_max"] if pd.notna(row["salary_max"]) else None
                    ),
                    "salary_avg": (
                        row["salary_avg"] if pd.notna(row["salary_avg"]) else None
                    ),
                }
            )

    jobs_df = pd.DataFrame(job_data)
    jobs_df.to_sql("f_job_offers", conn, if_exists="replace", index=False)
    logger.info(f"Offres d'emploi chargées: {len(jobs_df)}")


def load_github_trends(conn):
    """Charge les tendances GitHub"""
    logger.info("Chargement des tendances GitHub")

    df = pd.read_csv(f"{CLEAN_DATA_DIR}/github_trends_clean.csv")
    source_map = (
        pd.read_sql("SELECT source_name, id_source FROM d_source", conn)
        .set_index("source_name")["id_source"]
        .to_dict()
    )

    github_data = []
    for _, row in df.iterrows():
        created_date = pd.to_datetime(row["created_at"], errors="coerce")
        date_key = (
            created_date.strftime("%Y-%m-%d")
            if pd.notna(created_date)
            else "2024-06-30"
        )

        skill_id = conn.execute(
            "SELECT id_skill FROM d_skill WHERE tech_label = ?", (row["language"],)
        ).fetchone()
        id_skill = skill_id[0] if skill_id else 1
        id_source = source_map.get("GitHub", 2)

        github_data.append(
            {
                "id_skill": id_skill,
                "id_source": id_source,
                "date_key": date_key,
                "repo_name": row["name"],
                "stars": row["stars"],
                "forks": row["forks"],
                "popularity_score": row["popularity_score"],
            }
        )

    github_df = pd.DataFrame(github_data)
    github_df.to_sql("f_github_trends", conn, if_exists="replace", index=False)
    logger.info(f"Tendances GitHub chargées: {len(github_df)}")


def load_google_trends(conn):
    """Charge les tendances de recherche"""
    logger.info("Chargement des tendances Google")

    df = pd.read_csv(f"{CLEAN_DATA_DIR}/google_trends_clean.csv")
    source_map = (
        pd.read_sql("SELECT source_name, id_source FROM d_source", conn)
        .set_index("source_name")["id_source"]
        .to_dict()
    )

    trends_data = []
    for _, row in df.iterrows():
        skill_id = conn.execute(
            "SELECT id_skill FROM d_skill WHERE tech_label = ?", (row["keyword"],)
        ).fetchone()
        id_skill = skill_id[0] if skill_id else 1
        id_source = source_map.get("Google Trends", 3)

        trends_data.append(
            {
                "id_skill": id_skill,
                "id_source": id_source,
                "date_key": row["date"],
                "interest_value": row["interest_value"],
            }
        )

    trends_df = pd.DataFrame(trends_data)
    trends_df.to_sql("f_search_trends", conn, if_exists="replace", index=False)
    logger.info(f"Tendances recherche chargées: {len(trends_df)}")


def load_stackoverflow_survey(conn):
    """Charge l'enquête Stack Overflow"""
    logger.info("Chargement enquête Stack Overflow")

    df = pd.read_csv(f"{CLEAN_DATA_DIR}/stackoverflow_survey_clean.csv")
    country_map = {"Germany": 1, "France": 2, "Netherlands": 3, "Spain": 4, "Italy": 5, "Poland": 6, "Belgium":7, "Austria": 8, "Switzerland":9}
    source_map = (
        pd.read_sql("SELECT source_name, id_source FROM d_source", conn)
        .set_index("source_name")["id_source"]
        .to_dict()
    )

    survey_data = []
    for _, row in df.iterrows():
        id_country = country_map.get(row.get("Country"), 1)
        id_source = source_map.get("Stack Overflow", 4)

        survey_data.append(
            {
                "id_country": id_country,
                "id_source": id_source,
                "salary": row.get("CompTotal"),
                "years_experience": row.get("YearsCodePro"),
                "dev_type": row.get("DevType"),
                "languages_used": row.get("LanguageHaveWorkedWith"),
            }
        )

    survey_df = pd.DataFrame(survey_data)
    survey_df.to_sql("f_survey_responses", conn, if_exists="replace", index=False)
    logger.info(f"Enquête Stack Overflow chargée: {len(survey_df)}")


def create_indexes(conn):
    """Crée les index pour optimiser les requêtes"""
    logger.info("Création des index")

    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_job_country ON f_job_offers(id_country)",
        "CREATE INDEX IF NOT EXISTS idx_job_skill ON f_job_offers(id_skill)",
        "CREATE INDEX IF NOT EXISTS idx_job_date ON f_job_offers(date_key)",
        "CREATE INDEX IF NOT EXISTS idx_github_skill ON f_github_trends(id_skill)",
        "CREATE INDEX IF NOT EXISTS idx_trends_skill ON f_search_trends(id_skill)",
        "CREATE INDEX IF NOT EXISTS idx_survey_country ON f_survey_responses(id_country)",
    ]

    for idx in indexes:
        conn.execute(idx)

    conn.commit()
    logger.info("Index créés")


def main():
    """Fonction principale de chargement DWH"""
    start_time = datetime.now()
    logger.info("Démarrage chargement Data Warehouse")

    db_path = f"{DWH_DIR}/talentinsight.db"
    conn = sqlite3.connect(db_path)

    try:
        create_dwh_schema(conn)
        load_dimensions(conn)
        load_job_offers(conn)
        load_github_trends(conn)
        load_google_trends(conn)
        load_stackoverflow_survey(conn)
        create_indexes(conn)

        # Statistiques finales
        tables = [
            "d_country",
            "d_skill",
            "d_source",
            "f_job_offers",
            "f_github_trends",
            "f_search_trends",
            "f_survey_responses",
        ]
        for table in tables:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            logger.info(f"Table {table}: {count} lignes")

    finally:
        conn.close()

    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"Data Warehouse chargé en {duration.total_seconds():.1f}s")
    logger.info(f"Base de données: {db_path}")


if __name__ == "__main__":
    main()
