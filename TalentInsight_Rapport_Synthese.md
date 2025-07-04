# TalentInsight - Rapport Technique
## Pipeline ETL et API pour la Cartographie de l'Emploi Tech

**Date :** 4 juillet 2025 | **Durée :** 4 jours

---

## 1. Collecte des Données (01_scrape.py)

### Sources implémentées
- **Adzuna API :** Scraping avec clés API et gestion rate limiting
- **Indeed/LinkedIn :** Utilisation python-jobspy pour contourner protections
- **GitHub API :** Trending repositories avec authentification token
- **Google Trends :** pytrends pour mots-clés tech par pays
- **Stack Overflow :** Processing survey 2024 développeurs

```python
def scrape_adzuna(country_code, query):
    url = f"https://api.adzuna.com/v1/api/jobs/{country_code}/search"
    params = {'app_id': os.getenv('ADZUNA_APP_ID'), 'what': query}
    response = requests.get(url, params=params)
    return response.json()['results']
```

**Volumétrie :** 9,400+ documents collectés (offres emploi, repos GitHub, tendances)

## 2. Ingestion Data Lake (02_feeder.py)

### Architecture MongoDB
Collections séparées par source avec validation basique et metadata d'ingestion.

```python
def insert_documents(collection_name, data):
    for doc in data:
        doc['_source_file'] = collection_name
        doc['_ingestion_date'] = datetime.now()
    collection.insert_many(data)
```

**Résultat :** 6 collections MongoDB alimentées avec logs détaillés

## 3. Nettoyage et Transformation (03_clean_mongodb.py)

### Algorithmes implémentés
- **Extraction salaires :** Regex pour patterns "50k€", "€45,000-65,000"
- **Extraction compétences :** Mapping depuis titres d'offres
- **Suppression colonnes parasites :** URLs, logos, métadonnées inutiles
- **Filtrage strict :** Seules offres avec compétences identifiées conservées

```python
def extract_skills(title):
    skills = []
    if "python" in title.lower(): skills.append("Python")
    if "react" in title.lower(): skills.append("React")
    return skills

# Filtrage final strict
df = df[df["skills"].apply(lambda x: len(x) > 0)]
```

**Résultat :** 804 offres qualifiées avec compétences extraites (85% taux réussite estimé)

## 4. Data Warehouse (04_load_dwh.py)

### Schéma étoile SQLite
Construction d'un DWH avec tables de dimensions (pays, compétences, sources) et tables de faits (offres emploi, trends).

```python
def create_star_schema():
    cursor.execute("""CREATE TABLE d_country (
        id_country INTEGER PRIMARY KEY, iso2 TEXT, country_name TEXT)""")
    cursor.execute("""CREATE TABLE f_job_offers (
        id_job INTEGER PRIMARY KEY, title TEXT, salary_avg REAL,
        id_country INTEGER, id_skill INTEGER)""")
```

**Résultat :** 7 tables créées avec 804 offres emploi + dimensions de référence

## 5. API REST Django

### Endpoints analytiques
Django REST Framework avec models mappés sur DWH et endpoints custom pour analyses.

```python
class JobOfferViewSet(viewsets.ModelViewSet):
    @action(detail=False)
    def salary_by_country(self, request):
        cursor.execute("SELECT AVG(salary_avg) FROM f_job_offers WHERE...")
        return Response({'avg_salary': result[0]})
```

**Fonctionnalités :** Authentification token, pagination, endpoints analytiques

## 6. Automatisation

### Orchestration pipeline
Script bash modulaire avec logging et automatisation cron/launchd.

```bash
run_step() {
    echo "[$(date)] Démarrage: $1"
    python3 "scripts/$1"
    echo "[$(date)] Terminé: $1"
}
```

**Déploiement :** Crontab quotidien + LaunchD hebdomadaire + script installation

## 7. Métriques et Validation

### Performances mesurées (estimation tests manuels)
- **Pipeline ETL complet :** ~110s end-to-end (scraping + nettoyage + DWH)
- **API response time :** <300ms sur endpoints analytiques testés
- **Qualité données :** ~85% taux extraction salaires/compétences réussi
- **Coverage :** 5 pays × 9 compétences × 6 sources = couverture multi-dimensionnelle

### Tests validation effectués
- Exécution pipeline complet via run_pipeline.sh
- Tests endpoints API via Postman collection fournie
- Validation schéma DWH et export SQL généré
- Tests automatisation cron et launchd sur macOS

### Architecture livrée
```bash
TalentInsight/
├── scripts/          # Pipeline ETL 4 étapes
├── api/             # Django REST + authentification
├── datasets_clean/  # CSV nettoyés exportés  
├── dwh/            # SQLite Data Warehouse
├── automation/     # Cron + LaunchD + install
└── sql/           # Export complet DWH
```

**Stack technique :** Python 3.11 + Django REST + MongoDB + SQLite + Shell scripts

---

## Conclusion

Pipeline ETL opérationnel en 4 jours avec architecture modulaire. **9,400+ documents collectés → 804 offres qualifiées** avec extraction automatique salaires et compétences tech.

**Livré :** Data Warehouse structuré + API REST + automatisation système complète.

**Métriques estimées** basées sur tests manuels et validation fonctionnelle du pipeline complet.

---

*TalentInsight v1.0 - Data Engineering Team - 4 juillet 2025*
