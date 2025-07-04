# TalentInsight API - Plateforme de Cartographie du Marché de l'Emploi Tech en Europe

### Collaborateur :
Saad SHAHZAD, Thomas COUTAREL, Thomas YU, Noam BOULZE

## Description

TalentInsight est une API REST Django développée pour le programme "TalentInsight" de la Commission européenne. Cette plateforme collecte, nettoie, centralise et expose via une API REST des données du marché de l'emploi technologique en Europe à partir de 4 sources de données hétérogènes.

## Objectifs

- Cartographie du marché de l'emploi Tech européen
- Analyse des tendances technologiques et des salaires
- Centralisation de données multi-sources
- API REST moderne avec authentification par token
- Data Lake et Data Warehouse structurés
- Endpoints analytiques pour les insights

## Architecture

### Pipeline de données
```
Sources → Scraping → Nettoyage → Data Lake → Data Warehouse → API REST
```

### Stack technique
- Backend: Django 5.1 + Django REST Framework
- Base de données: SQLite (Data Warehouse)
- Authentification: Token-based authentication
- Python 3.11

## Sources de données

1. **Adzuna Jobs API** - Offres d'emploi européennes
2. **GitHub API** - Tendances des repositories tech
3. **Google Trends** - Intérêt pour les technologies
4. **Stack Overflow Survey** - Données de salaires et compétences (dataset 2024)
5. **Indeed Jobs** - Offres d'emploi tech en Europe (via JobSpy)

## Installation et utilisation

### 1. Installation
```bash
git clone [repository-url]
cd jobtech
pip install -r requirements.txt
```

### 2. Exécution du pipeline ETL
```bash
python3.11 scripts/01_scrape.py
python3.11 scripts/02_feeder.py
python3.11 scripts/03_clean_mongodb.py
python3.11 scripts/04_load_dwh.py
```

### 3. Génération d'un token d'API
```bash
python3.11 manage.py create_token
```

### 4. Lancement de l'API
```bash
python3.11 manage.py runserver 8000
```

### 5. Accès à la documentation
- Documentation Swagger UI : http://localhost:8000/api/schema/swagger-ui/
- Documentation ReDoc : http://localhost:8000/api/schema/redoc/

## Endpoints de l'API

### Endpoints de référence
- `GET /api/v1/countries/` - Liste des pays
- `GET /api/v1/skills/` - Liste des compétences
- `GET /api/v1/sources/` - Liste des sources de données
- `GET /api/v1/companies/` - Liste des entreprises

### Endpoints de données
- `GET /api/v1/jobs/` - Liste des offres d'emploi
- `GET /api/v1/jobs/?country=FR` - Filtrage par pays

### Endpoints analytiques
- `GET /api/v1/jobs/salary_daily/` - Statistiques salariales quotidiennes
- `GET /api/v1/jobs/skill_trends/` - Tendances des compétences tech

## Authentification

Toutes les requêtes nécessitent un token d'authentification dans le header :
```
Authorization: Token [votre-token]
```

## Structure du Data Warehouse

### Tables de dimension
- `d_country` - Référentiel des pays
- `d_skill` - Référentiel des compétences
- `d_source` - Référentiel des sources
- `d_company` - Référentiel des entreprises
- `d_date` - Dimension temporelle

### Tables de faits
- `f_job_offers` - Offres d'emploi collectées
- `f_github_trends` - Tendances GitHub
- `f_search_trends` - Tendances de recherche Google
- `f_survey_responses` - Réponses d'enquêtes

## Livrables du projet

1. Repository Git avec code source complet
2. API REST fonctionnelle avec authentification
3. Data Warehouse SQLite avec données nettoyées
4. Scripts ETL pour le pipeline de données
5. Documentation technique (ce README)
6. Endpoints analytiques pour insights métier

## Technologies utilisées

- Python 3.11
- Django 5.1
- Django REST Framework
- SQLite
- pandas, requests, BeautifulSoup
- python-decouple
