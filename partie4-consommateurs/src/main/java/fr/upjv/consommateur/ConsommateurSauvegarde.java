package fr.upjv.consommateur;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import fr.upjv.commun.MesureCapteur;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * ╔══════════════════════════════════════════════════════════════╗
 * ║  CONSOMMATEUR DE SAUVEGARDE                                  ║
 * ║  Personne 4 – Projet ISRI M1 2026                           ║
 * ╠══════════════════════════════════════════════════════════════╣
 * ║  Rôle : lire chaque message du topic Kafka et l'insérer     ║
 * ║         en base PostgreSQL via JDBC.                         ║
 * ╠══════════════════════════════════════════════════════════════╣
 * ║  Flux de données :                                           ║
 * ║  ProducteurApiWeb → Kafka (capteurs-topic)                   ║
 * ║      → ConsommateurSauvegarde → PostgreSQL (temperatures)    ║
 * ╠══════════════════════════════════════════════════════════════╣
 * ║  Variables d'environnement (configurables via Docker) :      ║
 * ║    KAFKA_BROKER   (défaut : localhost:9092)                  ║
 * ║    KAFKA_TOPIC    (défaut : capteurs-topic)                  ║
 * ║    DB_HOST        (défaut : localhost)                       ║
 * ║    DB_PORT        (défaut : 5432)                            ║
 * ║    DB_NAME        (défaut : capteurs_db)                     ║
 * ║    DB_USER        (défaut : user)                            ║
 * ║    DB_PASSWORD    (défaut : password)                        ║
 * ╚══════════════════════════════════════════════════════════════╝
 */
public class ConsommateurSauvegarde {

    // ── Logger Log4J ──────────────────────────────────────────────────────
    // LogManager.getLogger() récupère le logger configuré dans log4j2.xml.
    // Les logs seront écrits à la fois sur la console ET dans un fichier.
    private static final Logger logger = LogManager.getLogger(ConsommateurSauvegarde.class);

    // AJOUT DES METRIQUES PROMETHEUS :
    private static final Gauge tempBruteGauge = Gauge.build()
            .name("capteur_temperature_celsius")
            .help("Température brute instantanée par ville")
            .labelNames("ville")
            .register();

    private static final Counter kafkaMessagesCounter = Counter.build()
            .name("kafka_messages_received_total")
            .help("Nombre total de messages reçus depuis le bus Kafka")
            .register();

    // ── Configuration via variables d'environnement ───────────────────────
    // getOrDefault permet de fonctionner en local (valeurs par défaut)
    // ET en Docker (variables injectées dans le conteneur).
    private static final String KAFKA_BROKER =
            System.getenv().getOrDefault("KAFKA_BROKER", "localhost:9092");
    private static final String KAFKA_TOPIC  =
            System.getenv().getOrDefault("KAFKA_TOPIC",  "capteurs-topic");

    private static final String DB_URL =
            "jdbc:postgresql://"
            + System.getenv().getOrDefault("DB_HOST",     "localhost")
            + ":"
            + System.getenv().getOrDefault("DB_PORT",     "5432")
            + "/"
            + System.getenv().getOrDefault("DB_NAME",     "capteurs_db");
    private static final String DB_USER =
            System.getenv().getOrDefault("DB_USER",     "user");
    private static final String DB_PASSWORD =
            System.getenv().getOrDefault("DB_PASSWORD", "password");

    // ── Jackson : désérialisation JSON → MesureCapteur ───────────────────
    // JavaTimeModule : indispensable pour parser les LocalDateTime
    // (champ "timestamp" du message JSON).
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    // ── Requête SQL d'insertion ───────────────────────────────────────────
    // Simple INSERT : la clé primaire est le couple (ville, horodatage).
    // Chaque mesure a un timestamp différent → chaque ligne est unique.
    // On conserve ainsi tout l'historique des mesures par ville.
    private static final String SQL_INSERT =
            "INSERT INTO temperatures (ville, valeur, horodatage) " +
            "VALUES (?, ?, ?)";

    // ────────────────────────────────────────────────────────────────────
    public static void main(String[] args) {

        // AJOUT : Démarrage du serveur Prometheus sur le port 8082
        try {
            new HTTPServer(8082);
            logger.info("🔥 Serveur de métriques Prometheus démarré sur le port 8082");
        } catch (Exception e) {
            logger.error("❌ Impossible de démarrer le serveur Prometheus port 8082", e);
        }

        logger.info("╔══════════════════════════════════════════════════════╗");

        logger.info("╔══════════════════════════════════════════════════════╗");
        logger.info("║   CONSOMMATEUR SAUVEGARDE – démarrage                ║");
        logger.info("╠══════════════════════════════════════════════════════╣");
        logger.info("║   Kafka  : {}",  KAFKA_BROKER);
        logger.info("║   Topic  : {}",  KAFKA_TOPIC);
        logger.info("║   DB URL : {}",  DB_URL);
        logger.info("╚══════════════════════════════════════════════════════╝");

        // ── Connexion PostgreSQL ──────────────────────────────────────────
        // On ouvre UNE connexion persistante pour toute la durée du programme.
        // Elle est déclarée dans un try-with-resources : fermée automatiquement
        // à la fin, même en cas d'exception.
        try (Connection connexion = connecterBDD()) {

            logger.info("✅ Connexion PostgreSQL établie sur {}", DB_URL);

            // ── Configuration du KafkaConsumer ────────────────────────────
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  KAFKA_BROKER);

            // group.id : identifiant du groupe de consommateurs.
            // Kafka retient l'offset (position) pour chaque groupe.
            // Si ce consommateur redémarre, il reprend là où il s'était arrêté.
            props.put(ConsumerConfig.GROUP_ID_CONFIG,           "groupe-sauvegarde");

            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());

            // auto.offset.reset = "earliest" :
            // Si ce groupe n'a jamais consommé ce topic (première exécution),
            // on lit depuis le début. Utile pour ne pas rater de messages.
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  "earliest");

            // ── Boucle de consommation ─────────────────────────────────────
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {

                consumer.subscribe(List.of(KAFKA_TOPIC));
                logger.info("📡 Abonné au topic '{}'", KAFKA_TOPIC);

                // Shutdown hook : arrêt propre avec Ctrl+C
                // Sans ça, consumer.poll() serait bloqué indéfiniment.
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    logger.info("⛔ Arrêt demandé — fermeture du consumer...");
                    consumer.wakeup(); // interrompt le poll() en cours
                }));

                while (true) {
                    // poll(Duration) : attend jusqu'à 1 seconde pour de nouveaux messages.
                    // Retourne un lot (batch) de ConsumerRecord.
                    ConsumerRecords<String, String> records =
                            consumer.poll(Duration.ofSeconds(1));

                    for (ConsumerRecord<String, String> record : records) {
                        traiterMessage(record, connexion);
                    }
                }
            }

        } catch (org.apache.kafka.common.errors.WakeupException e) {
            // Exception normale lors d'un arrêt propre via wakeup()
            logger.info("Consumer arrêté proprement.");
        } catch (SQLException e) {
            logger.error("❌ Impossible de se connecter à PostgreSQL : {}", e.getMessage(), e);
        }
    }

    /**
     * Traite un message Kafka : désérialise le JSON puis insère en base.
     *
     * Étapes :
     *  1. Désérialisation JSON → MesureCapteur (via Jackson)
     *  2. Extraction de la ville depuis le champ "source" ("api-web:Paris" → "Paris")
     *  3. Insertion UPSERT en base PostgreSQL
     *  4. Logging du résultat
     *
     * @param record    le message Kafka brut (clé + valeur JSON)
     * @param connexion la connexion JDBC active
     */
    private static void traiterMessage(ConsumerRecord<String, String> record,
                                       Connection connexion) {
        try {
            // ── 1. Désérialisation JSON ───────────────────────────────────
            // MAPPER.readValue() convertit la String JSON en objet MesureCapteur.
            // Jackson utilise les annotations @JsonProperty de la classe.
            MesureCapteur mesure = MAPPER.readValue(record.value(), MesureCapteur.class);

            logger.debug("📨 Message reçu : {}", mesure);

            // ── 2. Extraction de la ville ─────────────────────────────────
            // source = "api-web:Paris" → ville = "Paris"
            String ville = mesure.getVille();

            // ── 3. Insertion en base (UPSERT) ─────────────────────────────
            // PreparedStatement : protège contre les injections SQL
            // et améliore les performances (requête pré-compilée).
            try (PreparedStatement ps = connexion.prepareStatement(SQL_INSERT)) {
                ps.setString(1, ville);
                ps.setDouble(2, mesure.getValeur());
                ps.setTimestamp(3, Timestamp.valueOf(mesure.getTimestamp()));

                int lignes = ps.executeUpdate();

                // ── 4. Logging ────────────────────────────────────────────
                if (lignes > 0) {
                    logger.info("💾 Sauvegardé → ville={}, valeur={}{}",
                            ville, mesure.getValeur(), mesure.getUnite());

                    // AJOUTS : Enregistrement dans les métriques Prometheus
                    tempBruteGauge.labels(ville).set(mesure.getValeur());
                    kafkaMessagesCounter.inc();
                }
            }

        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            // Message malformé : on logue l'erreur et on continue
            // (on ne veut pas planter le consumer pour un seul mauvais message)
            logger.error("❌ Impossible de désérialiser le message : {}", record.value(), e);
        } catch (SQLException e) {
            logger.error("❌ Erreur insertion PostgreSQL : {}", e.getMessage(), e);
        }
    }

    /**
     * Crée et retourne une connexion JDBC à PostgreSQL.
     * Réessaie toutes les 5 secondes si la base n'est pas encore prête.
     * Utile en Docker : PostgreSQL peut mettre quelques secondes à démarrer.
     *
     * @return une connexion JDBC active
     * @throws SQLException si la connexion échoue après plusieurs tentatives
     */
    private static Connection connecterBDD() throws SQLException {
        int tentatives = 0;
        while (tentatives < 10) {
            try {
                return DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            } catch (SQLException e) {
                tentatives++;
                logger.warn("⏳ PostgreSQL pas encore prêt (tentative {}/10), attente 5s...",
                        tentatives);
                try { Thread.sleep(5_000); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        throw new SQLException("Impossible de se connecter à PostgreSQL après 10 tentatives.");
    }
}
