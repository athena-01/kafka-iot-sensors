#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "⛔ Arrêt de tous les services..."
docker compose -f partie4-consommateurs/docker/docker-compose.yml -p docker down --remove-orphans 2>/dev/null || true
docker compose -f partie1-infrastructure/docker/zk-single-kafka-single.yml -p docker down --remove-orphans 2>/dev/null || true
docker network rm reseau-isri 2>/dev/null || true
echo "✅ Tous les services sont arrêtés."