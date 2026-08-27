# Partie 2 – Producteurs avec Virtual Threads

**Module :** Programmation Évènementielle – Master ISRI M1 2026
**Auteur :** Personne 2
**Dépend de :** Partie 1 (Kafka + ZooKeeper opérationnels)

---

## Vue d'ensemble

Ce module implémente la production de données vers le bus Kafka.
Il propose **deux versions** :

| Version | Classe principale | Données | Usage |
|---|---|---|---|
| **Base** | `ProducteurVirtuel` | Simulées (température + tension) | Démo, tests |
| **Évoluée** | `ProducteurApiWeb` | Réelles (OpenWeatherMap) | Version bonus |

---

## Architecture du code

```
partie2-producteurs/
└── src/main/java/fr/upjv/producteur/
    ├── commun/                          ← Partagé entre les deux versions
    │   ├── MesureCapteur.java           ← POJO sérialisable (Jackson)
    │   ├── MessageSerialiser.java       ← Utilitaire Jackson (Singleton)
    │   └── ProfilProduction.java        ← Enum des profils (Strategy)
    │
    ├── base/                            ← Version de base (données simulées)
    │   ├── Capteur.java                 ← Classe abstraite (Template Method)
    │   ├── CapteurTemperature.java      ← Sous-classe concrète
    │   ├── CapteurTension.java          ← Sous-classe concrète
    │   ├── CapteurFactory.java          ← Fabrique de capteurs (Factory Method)
    │   └── ProducteurVirtuel.java       ← Main : 2 groupes × Virtual Threads
    │
    └── evolue/                          ← Version évoluée (données réelles)
        └── ProducteurApiWeb.java        ← Main : OpenWeatherMap → Kafka
```

---

## Design Patterns implémentés

### Template Method (`Capteur.java`)
```
Capteur (abstraite)
│   produireMesure()         ← méthode finale, squelette fixé
│     ├── genererValeur()    ← ABSTRAITE → implémentée par chaque sous-classe
│     ├── appliquerBruit()   ← définie ici, commune à tous
│     └── new MesureCapteur  ← définie ici, commune à tous
│
├── CapteurTemperature       → dérive progressive ±10°C autour de 22°C
└── CapteurTension           → gaussienne autour de 230V (norme EN 50160)
```
**Avantage :** le bruit physique, l'horodatage et la construction JSON
sont écrits une seule fois. Chaque capteur n'écrit que sa physique propre.

### Factory Method (`CapteurFactory.java`)
```java
CapteurFactory.creerFlotte(10)
// → 6 CapteurTemperature (profils Lent/Normal)
// → 4 CapteurTension     (profils Rapide/TrèsRapide)
```
**Avantage :** `ProducteurVirtuel` ne connaît pas les sous-classes concrètes.
Pour ajouter un `CapteurPuissance`, on ajoute un `case` dans la factory.

### Strategy (`ProfilProduction.java`)
```java
enum ProfilProduction {
    LENT        (5000ms, bruit 1%),
    NORMAL      (2000ms, bruit 2%),
    RAPIDE      (1000ms, bruit 5%),
    TRES_RAPIDE ( 500ms, bruit 10%)
}
```
**Avantage :** on change la fréquence et le bruit d'un capteur
sans toucher à sa classe.

---

## Virtual Threads

Chaque capteur tourne dans un **Virtual Thread** dédié :
```java
ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
executor.submit(() -> boucleProduction(capteur, producer));
```

| | Platform Thread | Virtual Thread |
|---|---|---|
| Mémoire | ~1 Mo par thread | Quelques Ko |
| 100 capteurs | ~100 Mo RAM | ~quelques Mo |
| `Thread.sleep()` | Bloque le thread OS | Libère le thread OS |

Le `KafkaProducer` est **thread-safe** : une seule instance est partagée
par tous les Virtual Threads, ce qui est la bonne pratique.

---

## Format JSON produit

```json
{
  "capteurId" : 3,
  "type"      : "temperature",
  "valeur"    : 23.47,
  "unite"     : "°C",
  "source"    : "simulation",
  "timestamp" : "2026-05-25T14:32:01.123"
}
```

Le champ `source` vaut `"simulation"` (version de base) ou `"api-web:Amiens"`
(version évoluée) pour que la Personne 4 puisse distinguer les origines.

---

## Démarrage

### Prérequis
1. Partie 1 démarrée :
   ```bash
   cd partie1-infrastructure/docker
   docker-compose -f zk-single-kafka-single.yml up -d
   ```
2. Topic créé (lancer `CreationTopic` de la Partie 1 une seule fois)

### Depuis IntelliJ (développement)
- Run → `ProducteurVirtuel` (version de base)
- Run → `ProducteurApiWeb`  (version évoluée, nécessite la clé API)

### Depuis le terminal (fat JAR)
```bash
# Compiler
mvn package

# Version de base
java -jar target/producteur-base-fat.jar

# Version évoluée
export OPENWEATHER_API_KEY=votre_cle_ici
java -jar target/producteur-api-fat.jar
```

### Via Docker (intégration avec Partie 1)
```bash
# Version de base
docker build -t producteur-base -f docker/Dockerfile.base .
docker run --network=host -e KAFKA_BROKER=localhost:9092 producteur-base

# Version évoluée
docker build -t producteur-api -f docker/Dockerfile.api .
docker run --network=host \
  -e KAFKA_BROKER=localhost:9092 \
  -e OPENWEATHER_API_KEY=votre_cle \
  producteur-api
```

---

## Configuration

### Version de base (`ProducteurVirtuel.java`)
```java
private static final int NB_CAPTEURS_PAR_GROUPE = 5;  // capteurs par groupe
private static final int NB_GROUPES = 2;               // nombre de "producteurs"
// Total : 10 capteurs, 2 Virtual Thread pools
```

### Variables d'environnement (les deux versions)
| Variable | Défaut | Description |
|---|---|---|
| `KAFKA_BROKER` | `localhost:9092` | Adresse du broker Kafka |
| `KAFKA_TOPIC` | `capteurs-topic` | Nom du topic |
| `OPENWEATHER_API_KEY` | *(requis pour version évoluée)* | Clé API OpenWeatherMap |

---

## Obtenir une clé OpenWeatherMap (gratuit)

1. Aller sur https://home.openweathermap.org/users/sign_up
2. Créer un compte (email suffisant)
3. Aller dans "API keys" → copier la clé générée
4. La clé est active après ~10 minutes
5. Plan gratuit : 1 000 appels/jour, largement suffisant

---

## Ce que la Personne 4 doit savoir

Le format JSON est **stable et contractuel**. La Personne 4 peut compter sur :
- `capteurId` → int, identifiant unique
- `type` → String, "temperature" ou "tension"
- `valeur` → double, arrondi à 2 décimales
- `unite` → String, "°C" ou "V"
- `source` → String, origine de la donnée
- `timestamp` → String ISO-8601 `yyyy-MM-dd'T'HH:mm:ss.SSS`

Pour désérialiser avec Jackson :
```java
MesureCapteur mesure = MessageSerialiser.fromJson(json, MesureCapteur.class);
```
