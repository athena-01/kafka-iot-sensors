#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "╔════════════════════════════════════════════════════════╗"
echo "║   DÉMARRAGE AUTOMATIQUE GLOBAL - PROJET KAFKA 2026     ║"
echo "╚════════════════════════════════════════════════════════╝"

# ── ÉTAPE 0 : Nettoyage ──
echo "🧹 0. Nettoyage des anciens conteneurs..."
docker compose -f partie1-infrastructure/docker/zk-single-kafka-single.yml -p docker down --remove-orphans 2>/dev/null || true
docker compose -f partie4-consommateurs/docker/docker-compose.yml -p docker down --remove-orphans 2>/dev/null || true

# ── ÉTAPE 1 : Création du réseau ──
echo "🔌 1. Création du réseau Docker..."
docker network create reseau-isri 2>/dev/null || echo "   ↳ réseau déjà existant, on continue..."

# ── ÉTAPE 2 : Création du dossier de logs partagé ──
echo "📁 2. Création du dossier de logs partagé..."
mkdir -p logs

# ── ÉTAPE 3 : Compilation Maven ──
echo "🔨 3. Compilation Maven..."
mvn clean package -pl partie4-consommateurs -am -DskipTests -q
mvn clean package -pl partie2-producteurs -am -DskipTests -q 2>/dev/null || true

# ── ÉTAPE 4 : Infrastructure ──
echo "🌐 4. Lancement de l'infrastructure (Kafka, Prometheus, Grafana, Loki, Fluent Bit)..."
docker compose -f partie1-infrastructure/docker/zk-single-kafka-single.yml -p docker up -d --build

echo "⏳ Attente de 40 secondes pour la stabilisation de Kafka..."
sleep 40

# ── ÉTAPE 5 : Création du topic Kafka ──
echo "📦 5. Création du topic Kafka..."
mvn -f partie1-infrastructure/pom.xml exec:java \
    -Dexec.mainClass="fr.upjv.creationtopic.CreationTopic" \
    -DskipTests -q 2>&1 | grep -v "TopicExistsException" || true

# ── ÉTAPE 6 : Consommateurs + PostgreSQL ──
echo "💾 6. Lancement de PostgreSQL et des Consommateurs Java..."
docker compose -f partie4-consommateurs/docker/docker-compose.yml -p docker up -d --build

echo "⏳ Attente de 15 secondes pour la stabilisation..."
sleep 15

# ── VÉRIFICATION FINALE ──
echo ""
echo "📊 État des conteneurs :"
docker ps --format "table {{.Names}}\t{{.Status}}"

echo ""
echo "╔════════════════════════════════════════════════════════╗"
echo "║   ✅ SYSTÈME EN LIGNE !                                ║"
echo "║                                                        ║"
echo "║   📈 Grafana    : http://localhost:3000                ║"
echo "║      Login      : admin / admin                        ║"
echo "║   🔥 Prometheus : http://localhost:9090                ║"
echo "║   📋 Loki       : http://localhost:3100                ║"
echo "║   🌐 API Web    : http://localhost:8080                ║"
echo "╚════════════════════════════════════════════════════════╝"