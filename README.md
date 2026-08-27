# KafkaIsri2026 – Projet Programmation Évènementielle

**Master ISRI M1 2026** –

Dans ce système, des capteurs (température, tension) publient leurs mesures sur Kafka via des producteurs utilisant les Virtual Threads (Java), des consommateurs les traitent et les persistent en base, et une stack d'observabilité (Prometheus, Grafana, Loki) permet le monitoring et la visualisation en temps réel.

deux consommateurs appartiennent à des groupes Kafka différents (groupe-sauvegarde, groupe-moyenne) : ils reçoivent donc chacun l'intégralité des messages du topic, indépendamment l'un de l'autre.

ConsommateurSauvegarde : persiste chaque mesure brute en base PostgreSQL (table temperatures), et expose des métriques Prometheus (température brute par ville, nombre de messages reçus).
ConsommateurMoyenne : calcule une moyenne glissante (fenêtre de N mesures, configurable) par ville, logue le résultat via Log4J, et expose la moyenne lissée en métrique Prometheus.

## Design patterns utilisés
Template Method : structure commune des producteurs de capteurs, avec des étapes spécifiques à chaque type de capteur.
Factory : création des producteurs selon le type de capteur.
Strategy : logique de génération/traitement des données interchangeable selon le capteur.

## Stack technique
Java 21 (Virtual Threads, Project Loom)
Apache Kafka
PostgreSQL (JDBC)
Prometheus (métriques)
Grafana (dashboards)
Loki (agrégation de logs)
Log4J
Docker / Docker Compose
Maven

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
git clone <url-du-repo>
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
