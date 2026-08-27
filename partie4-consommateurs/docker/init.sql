-- ╔══════════════════════════════════════════════════════════════════╗
-- ║  SCHÉMA DE BASE DE DONNÉES – Projet ISRI M1 2026                ║
-- ║  Personne 4                                                      ║
-- ╠══════════════════════════════════════════════════════════════════╣
-- ║  Ce fichier est exécuté automatiquement par PostgreSQL           ║
-- ║  au premier démarrage du conteneur Docker, grâce au volume :     ║
-- ║    ./init.sql:/docker-entrypoint-initdb.d/init.sql               ║
-- ╚══════════════════════════════════════════════════════════════════╝

-- ════════════════════════════════════════════════════════════════
-- TABLE 1 : temperatures
-- Stocke toutes les mesures reçues pour chaque ville.
--
-- Colonnes :
--   ville      → nom extrait du champ "source"
--                (ex : "api-web:Paris" → "Paris")
--   valeur     → température mesurée (REAL = float 32 bits, suffisant pour °C)
--   horodatage → moment de la mesure
--
-- Clé primaire composite (ville, horodatage) :
--   Un même couple ville+horodatage ne peut pas apparaître deux fois.
--   Chaque nouvelle mesure d'une ville (timestamp différent) est insérée
--   comme une nouvelle ligne → on conserve tout l'historique.
-- ════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS temperatures (
    ville      VARCHAR(50) NOT NULL,
    valeur     REAL        NOT NULL,
    horodatage TIMESTAMP   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ville, horodatage)
);

-- Index pour accélérer les requêtes par ville ou par date
CREATE INDEX IF NOT EXISTS idx_temperatures_ville
    ON temperatures (ville);
CREATE INDEX IF NOT EXISTS idx_temperatures_horodatage
    ON temperatures (horodatage DESC);

-- ════════════════════════════════════════════════════════════════
-- Données de test (optionnel – à commenter en production)
-- ════════════════════════════════════════════════════════════════
-- INSERT INTO temperatures (ville, valeur, horodatage)
-- VALUES ('Paris',  20.5, NOW()),
--        ('Lyon',   22.1, NOW()),
--        ('Amiens', 19.8, NOW());
