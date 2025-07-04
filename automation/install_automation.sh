#!/bin/bash

# Installation et configuration des services d'automatisation TalentInsight

set -e

JOBTECH_DIR="/Users/saad/jobtech"
AUTOMATION_DIR="$JOBTECH_DIR/automation"
LAUNCHD_DIR="$HOME/Library/LaunchAgents"

echo "=== Installation Automatisation TalentInsight ==="

# 1. Créer les dossiers nécessaires
mkdir -p "$JOBTECH_DIR/logs"
mkdir -p "$LAUNCHD_DIR"

# 2. Rendre le script principal exécutable
chmod +x "$JOBTECH_DIR/scripts/run_pipeline.sh"

# 3. Installation du service launchd (équivalent systemd sur Mac)
echo "Installation du service launchd..."
cp "$AUTOMATION_DIR/com.talentinsight.pipeline.plist" "$LAUNCHD_DIR/"

# Charger le service
launchctl unload "$LAUNCHD_DIR/com.talentinsight.pipeline.plist" 2>/dev/null || true
launchctl load "$LAUNCHD_DIR/com.talentinsight.pipeline.plist"

echo "Service launchd installé et chargé"

# 4. Configuration crontab (optionnel, pour plus de contrôle)
echo ""
echo "Pour installer les tâches cron (optionnel) :"
echo "crontab -e"
echo "Puis coller le contenu de automation/crontab_talentinsight.txt"

# 5. Test du pipeline
echo ""
echo "Test du pipeline..."
cd "$JOBTECH_DIR"
./scripts/run_pipeline.sh --help || echo "Usage: ./scripts/run_pipeline.sh [scrape|feed|clean|dwh|all]"

echo ""
echo "=== Installation terminée ==="
echo "Le pipeline s'exécutera automatiquement chaque dimanche à 2h du matin"
echo ""
echo "Commandes utiles :"
echo "- Voir les services : launchctl list | grep talentinsight"
echo "- Voir les logs : tail -f $JOBTECH_DIR/logs/*.log"
echo "- Exécution manuelle : ./scripts/run_pipeline.sh all"
echo "- Arrêter le service : launchctl unload $LAUNCHD_DIR/com.talentinsight.pipeline.plist"
