# KafkaIsri2026 – Projet Programmation Évènementielle

**Master ISRI M1 2026** –

## Structure du projet

| 1 | Personne 1 | Infrastructure Kafka, Docker Compose, topic |
| 2 | Personne 2 | Producteurs, Virtual Threads,  API Web |
| 3 | Personne 3 | Prometheus, Grafana, dashboards |
| 4 | Personne 4 | Consommateurs, PostgreSQL, moyennes glissantes, Log4J |


## Participants
1-GHANEM LAKHAL MAHMOUD-ZOBEIR

2-LOUHOUT GODIS ATHENA ADELANGE

3-DIALLO CHEICK OMAR

4-ROYO JULIEN

## Execution du code
### Prérequis
- Docker Desktop lancé
- Java 21 (JDK)
- Maven 3.x
### 1. Lancement
cd KafkaIsri2026

### 2. Lancer tout le système
chmod +x start.sh
./start.sh

### 3. Accéder aux interfaces
| Service    | URL                        | Login         |
|------------|----------------------------|---------------|
| Grafana    | http://localhost:3000       | admin / admin |
| Prometheus | http://localhost:9090       |               |
| Loki       | http://localhost:3100/ready |               |
### 4. Grafana — Visualiser les données
1. Ouvrir http://localhost:3000
2. Aller dans Dashboards → le dashboard "Kafka Monitoring"
   est préconfiguré automatiquement

### 5. Arrêter le système
./stop.sh
