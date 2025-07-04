#!/bin/bash

# Pipeline TalentInsight - Automatisation complète
# Usage: ./run_pipeline.sh [scrape|feed|clean|dwh|all]

set -e  # Arrêt en cas d'erreur

SCRIPT_DIR="/Users/saad/jobtech/scripts"
LOG_DIR="/Users/saad/jobtech/logs"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

# Créer dossier logs
mkdir -p "$LOG_DIR"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_DIR/pipeline_$TIMESTAMP.log"
}

run_scraping() {
    log "Démarrage du scraping..."
    cd "$SCRIPT_DIR"
    python 01_scrape.py 2>&1 | tee -a "$LOG_DIR/scraping_$TIMESTAMP.log"
    log "Scraping terminé"
}

run_feeding() {
    log "Démarrage de l'ingestion MongoDB..."
    cd "$SCRIPT_DIR"
    python 02_feeder.py 2>&1 | tee -a "$LOG_DIR/feeding_$TIMESTAMP.log"
    log "Ingestion terminée"
}

run_cleaning() {
    log "Démarrage du nettoyage..."
    cd "$SCRIPT_DIR"
    python 03_clean_mongodb.py 2>&1 | tee -a "$LOG_DIR/cleaning_$TIMESTAMP.log"
    log "Nettoyage terminé"
}

run_dwh() {
    log "Démarrage du chargement DWH..."
    cd "$SCRIPT_DIR"
    python 04_load_dwh.py 2>&1 | tee -a "$LOG_DIR/dwh_$TIMESTAMP.log"
    log "Chargement DWH terminé"
}

run_full_pipeline() {
    log "Démarrage du pipeline complet TalentInsight"
    
    start_time=$(date +%s)
    
    run_scraping
    run_feeding
    run_cleaning
    run_dwh
    
    end_time=$(date +%s)
    duration=$((end_time - start_time))
    
    log "Pipeline complet terminé en ${duration}s"
    
    # Statistiques finales
    log "Statistiques des fichiers générés:"
    for file in /Users/saad/jobtech/datasets_clean/*.csv; do
        if [ -f "$file" ]; then
            size=$(ls -lh "$file" | awk '{print $5}')
            lines=$(wc -l < "$file")
            log "   - $(basename "$file"): $lines lignes, $size"
        fi
    done
}

# Menu principal
case "${1:-all}" in
    "scrape")
        run_scraping
        ;;
    "feed")
        run_feeding
        ;;
    "clean")
        run_cleaning
        ;;
    "dwh")
        run_dwh
        ;;
    "all")
        run_full_pipeline
        ;;
    *)
        echo "Usage: $0 [scrape|feed|clean|dwh|all]"
        echo "  scrape - Scraping multi-sources"
        echo "  feed   - Ingestion dans MongoDB"
        echo "  clean  - Nettoyage des données"
        echo "  dwh    - Chargement Data Warehouse"
        echo "  all    - Pipeline complet (défaut)"
        exit 1
        ;;
esac
