#!/usr/bin/env python3
"""
02_clean.py - Nettoyage et normalisation des données TalentInsight
"""

import os
import json
import pandas as pd
import re
from datetime import datetime
from loguru import logger

RAW_DATA_DIR = "raw"
CLEAN_DATA_DIR = "datasets_clean"
os.makedirs(CLEAN_DATA_DIR, exist_ok=True)

logger.remove()
logger.add(f"{CLEAN_DATA_DIR}/cleaning.log", level="INFO", rotation="1 day")


def clean_adzuna_jobs():
    """Nettoie et normalise les données Adzuna"""
    logger.info("Nettoyage des données Adzuna")

    with open(f"{RAW_DATA_DIR}/adzuna_jobs.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    df = pd.DataFrame(data)

    # Normalisation pays
    country_mapping = {
        "fr": "France",
        "de": "Germany",
        "nl": "Netherlands",
        "es": "Spain",
        "it": "Italy",
    }
    df["country_name"] = df["country"].map(country_mapping)

    # Extraction compétences du titre
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

    # Nettoyage salaires
    df["salary_min"] = pd.to_numeric(df["salary_min"], errors="coerce")
    df["salary_max"] = pd.to_numeric(df["salary_max"], errors="coerce")
    df["salary_avg"] = (df["salary_min"] + df["salary_max"]) / 2

    # Nettoyage dates
    df["created"] = pd.to_datetime(df["created"], errors="coerce")
    df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce")

    # Sauvegarde
    df.to_csv(f"{CLEAN_DATA_DIR}/adzuna_jobs_clean.csv", index=False, encoding="utf-8")
    logger.info(f"Adzuna nettoyé: {len(df)} offres")


def clean_github_trends():
    """Nettoie et normalise les données GitHub"""
    logger.info("Nettoyage des données GitHub")

    with open(f"{RAW_DATA_DIR}/github_trends.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    df = pd.DataFrame(data)

    # Nettoyage dates
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")
    df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce")

    # Nettoyage métriques
    df["stars"] = pd.to_numeric(df["stars"], errors="coerce").fillna(0)
    df["forks"] = pd.to_numeric(df["forks"], errors="coerce").fillna(0)

    # Score popularité
    df["popularity_score"] = df["stars"] * 0.7 + df["forks"] * 0.3

    # Sauvegarde
    df.to_csv(
        f"{CLEAN_DATA_DIR}/github_trends_clean.csv", index=False, encoding="utf-8"
    )
    logger.info(f"GitHub nettoyé: {len(df)} repos")


def clean_google_trends():
    """Nettoie et normalise les données Google Trends"""
    logger.info("Nettoyage des données Google Trends")

    with open(f"{RAW_DATA_DIR}/google_trends.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    trends_list = []
    for item in data:
        keyword = item["keyword"]
        for date_str, value in item["interest_over_time"].items():
            trends_list.append(
                {
                    "keyword": keyword,
                    "date": date_str,
                    "interest_value": value,
                    "scraped_at": item["scraped_at"],
                }
            )

    df = pd.DataFrame(trends_list)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce")
    df["interest_value"] = pd.to_numeric(df["interest_value"], errors="coerce")

    df.to_csv(
        f"{CLEAN_DATA_DIR}/google_trends_clean.csv", index=False, encoding="utf-8"
    )
    logger.info(f"Google Trends nettoyé: {len(df)} points de données")


def clean_stackoverflow_survey():
    """Nettoie et normalise l'enquête Stack Overflow"""
    logger.info("Nettoyage des données Stack Overflow")

    # Lecture par chunks pour économiser la mémoire
    chunk_size = 10000
    df_chunks = []

    for chunk in pd.read_csv(
        f"{RAW_DATA_DIR}/stackoverflow_survey_2024.csv",
        chunksize=chunk_size,
        low_memory=False,
    ):
        # Filtrer les colonnes utiles
        useful_cols = [
            "Country",
            "LanguageHaveWorkedWith",
            "CompTotal",
            "Currency",
            "DevType",
            "YearsCodePro",
            "Employment",
            "EdLevel",
        ]

        available_cols = [col for col in useful_cols if col in chunk.columns]
        chunk_filtered = chunk[available_cols].copy()

        # Filtrer les pays européens
        eu_countries = ["Germany", "France", "Netherlands", "Spain", "Italy", "Poland"]
        if "Country" in chunk_filtered.columns:
            chunk_filtered = chunk_filtered[
                chunk_filtered["Country"].isin(eu_countries)
            ]

        if not chunk_filtered.empty:
            df_chunks.append(chunk_filtered)

    if df_chunks:
        df = pd.concat(df_chunks, ignore_index=True)

        # Nettoyage salaires
        if "CompTotal" in df.columns:
            df["CompTotal"] = pd.to_numeric(df["CompTotal"], errors="coerce")
            df = df[df["CompTotal"].notna()]
            df = df[(df["CompTotal"] > 10000) & (df["CompTotal"] < 500000)]

        # Extraction langages
        if "LanguageHaveWorkedWith" in df.columns:
            df["languages_list"] = (
                df["LanguageHaveWorkedWith"].fillna("").str.split(";")
            )

        df.to_csv(
            f"{CLEAN_DATA_DIR}/stackoverflow_survey_clean.csv",
            index=False,
            encoding="utf-8",
        )
        logger.info(f"Stack Overflow nettoyé: {len(df)} réponses")
    else:
        logger.warning("Aucune donnée Stack Overflow à nettoyer")


def create_dimension_tables():
    """Crée les tables de dimensions"""
    logger.info("Création des tables de dimensions")

    # Table pays
    countries_data = [
        {
            "iso2": "FR",
            "country_name": "France",
            "region": "Western Europe",
            "currency": "EUR",
        },
        {
            "iso2": "DE",
            "country_name": "Germany",
            "region": "Western Europe",
            "currency": "EUR",
        },
        {
            "iso2": "NL",
            "country_name": "Netherlands",
            "region": "Western Europe",
            "currency": "EUR",
        },
        {
            "iso2": "ES",
            "country_name": "Spain",
            "region": "Southern Europe",
            "currency": "EUR",
        },
        {
            "iso2": "IT",
            "country_name": "Italy",
            "region": "Southern Europe",
            "currency": "EUR",
        },
    ]

    countries_df = pd.DataFrame(countries_data)
    countries_df.to_csv(
        f"{CLEAN_DATA_DIR}/dim_countries.csv", index=False, encoding="utf-8"
    )

    # Table compétences
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

    skills_df = pd.DataFrame(skills_data)
    skills_df.to_csv(f"{CLEAN_DATA_DIR}/dim_skills.csv", index=False, encoding="utf-8")

    # Table sources
    sources_data = [
        {"source_name": "Adzuna"},
        {"source_name": "GitHub"},
        {"source_name": "Google Trends"},
        {"source_name": "Stack Overflow"},
    ]

    sources_df = pd.DataFrame(sources_data)
    sources_df.to_csv(
        f"{CLEAN_DATA_DIR}/dim_sources.csv", index=False, encoding="utf-8"
    )

    logger.info("Tables de dimensions créées")


def main():
    """Fonction principale de nettoyage"""
    start_time = datetime.now()
    logger.info("Démarrage nettoyage TalentInsight")

    clean_adzuna_jobs()
    clean_github_trends()
    clean_google_trends()
    clean_stackoverflow_survey()
    create_dimension_tables()

    end_time = datetime.now()
    duration = end_time - start_time

    logger.info(f"Nettoyage terminé en {duration.total_seconds():.1f}s")

    # Récapitulatif des fichiers nettoyés
    clean_files = [f for f in os.listdir(CLEAN_DATA_DIR) if f.endswith(".csv")]
    for file in clean_files:
        file_path = os.path.join(CLEAN_DATA_DIR, file)
        size = os.path.getsize(file_path)
        logger.info(f"Fichier nettoyé: {file} ({size} bytes)")


if __name__ == "__main__":
    main()
