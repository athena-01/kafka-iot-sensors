package fr.upjv.consommateur;

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

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * ╔══════════════════════════════════════════════════════════════╗
 * ║  CONSOMMATEUR DE MOYENNE FENÊTRÉE                            ║
 * ║  Personne 4 – Projet ISRI M1 2026                            ║
 * ╠══════════════════════════════════════════════════════════════╣
 * ║  Rôle : calculer une moyenne glissante (sliding window)      ║
 * ║         sur les N dernières mesures par ville, et la         ║
 * ║         logger à l'écran et dans un fichier via Log4J.       ║
 * ╠══════════════════════════════════════════════════════════════╣
 * ║  Flux de données :                                           ║
 * ║  ProducteurApiWeb → Kafka (capteurs-topic)                   ║
 * ║      → ConsommateurMoyenne → Log4J (console + fichier)       ║
 * ╠══════════════════════════════════════════════════════════════╣
 * ║  Principe de la fenêtre glissante :                          ║
 * ║  On garde une file (Deque) des N dernières valeurs           ║
 * ║  par ville. À chaque nouveau message, on ajoute la           ║
 * ║  valeur en queue, on retire la plus ancienne si la file      ║
 * ║  dépasse TAILLE_FENETRE, puis on recalcule la moyenne.       ║
 * ║                                                              ║
 * ║  Exemple avec TAILLE_FENETRE = 5, ville = Paris :            ║
 * ║    msg1: [21.5]                  → moy = 21.50               ║
 * ║    msg2: [21.5, 21.8]            → moy = 21.65               ║
 * ║    msg3: [21.5, 21.8, 22.1]      → moy = 21.80               ║
 * ║    msg4: [21.5, 21.8, 22.1, 21.9]→ moy = 21.83               ║
 * ║    msg5: [21.5, 21.8, 22.1,      → moy = 21.86               ║
 * ║           21.9, 22.0]                                        ║
 * ║    msg6: [21.8, 22.1, 21.9,      → moy = 21.96 (21.5 sorti)  ║
 * ║           22.0, 22.0]                                        ║
 * ╠══════════════════════════════════════════════════════════════╣
 * ║  Variables d'environnement (configurables via Docker) :      ║
 * ║    KAFKA_BROKER    (défaut : localhost:9092)                 ║
 * ║    KAFKA_TOPIC     (défaut : capteurs-topic)                 ║
 * ║    TAILLE_FENETRE  (défaut : 5)                              ║
 * ╚══════════════════════════════════════════════════════════════╝
 */
public class ConsommateurMoyenne {


    // ── Logger Log4J ──────────────────────────────────────────────────────
    private static final Logger logger = LogManager.getLogger(ConsommateurMoyenne.class);
    // AJOUT DE LA METRIQUE PROMETHEUS :
    private static final Gauge tempMoyenneGauge = Gauge.build()
            .name("capteur_temperature_moyenne_celsius")
            .help("Moyenne lissée des températures par ville")
            .labelNames("ville")
            .register();
    // ── Configuration ─────────────────────────────────────────────────────
    private static final String KAFKA_BROKER =
            System.getenv().getOrDefault("KAFKA_BROKER",   "localhost:9092");
    private static final String KAFKA_TOPIC =
            System.getenv().getOrDefault("KAFKA_TOPIC",    "capteurs-topic");

    // Taille de la fenêtre glissante (nombre de mesures retenues par ville)
    private static final int TAILLE_FENETRE =
            Integer.parseInt(System.getenv().getOrDefault("TAILLE_FENETRE", "5"));

    // ── Jackson ───────────────────────────────────────────────────────────
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    /**
     * Fenêtres glissantes par ville.
     *
     * Structure :
     *   Map<String, Deque<Double>>
     *     clé   = nom de la ville (ex: "Paris")
     *     valeur = file des N dernières valeurs (ex: [21.5, 21.8, 22.1])
     *
     * On utilise un HashMap car les messages de différentes villes
     * arrivent en parallèle et chaque ville a sa propre fenêtre.
     */
    private static final Map<String, Deque<Double>> fenetresParVille = new HashMap<>();

    // ─────────────────────────────────────────────────────────────────────
    public static void main(String[] args) {
        // AJOUT : Démarrage du serveur Prometheus sur le port 8081
        try {
            new HTTPServer(8081);
            logger.info("🔥 Serveur de métriques Prometheus démarré sur le port 8081");
        } catch (Exception e) {
            logger.error("❌ Impossible de démarrer le serveur Prometheus port 8081", e);
        }

        logger.info("╔══════════════════════════════════════════════════════╗");

        logger.info("╔══════════════════════════════════════════════════════╗");
        logger.info("║   CONSOMMATEUR MOYENNE FENÊTRÉE – démarrage          ║");
        logger.info("╠══════════════════════════════════════════════════════╣");
        logger.info("║   Kafka          : {}", KAFKA_BROKER);
        logger.info("║   Topic          : {}", KAFKA_TOPIC);
        logger.info("║   Taille fenêtre : {} mesures", TAILLE_FENETRE);
        logger.info("╚══════════════════════════════════════════════════════╝");

        // ── Configuration du KafkaConsumer ────────────────────────────────
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);

        // Groupe différent de ConsommateurSauvegarde :
        // Kafka distribue les messages indépendamment à chaque groupe.
        // Les deux consommateurs reçoivent donc TOUS les messages.
        props.put(ConsumerConfig.GROUP_ID_CONFIG,          "groupe-moyenne");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // ── Boucle de consommation ─────────────────────────────────────────
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {

            consumer.subscribe(List.of(KAFKA_TOPIC));
            logger.info("📡 Abonné au topic '{}'", KAFKA_TOPIC);

            // Shutdown hook : arrêt propre
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("⛔ Arrêt demandé — fermeture du consumer...");
                consumer.wakeup();
            }));

            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, String> record : records) {
                    traiterMessage(record);
                }
            }

        } catch (org.apache.kafka.common.errors.WakeupException e) {
            logger.info("Consumer arrêté proprement.");
        }
    }

    /**
     * Traite un message Kafka :
     *  1. Désérialise le JSON → MesureCapteur
     *  2. Met à jour la fenêtre glissante de la ville concernée
     *  3. Calcule la moyenne sur la fenêtre
     *  4. Logue le résultat (console + fichier via Log4J)
     *
     * @param record le message Kafka brut
     */
    private static void traiterMessage(ConsumerRecord<String, String> record) {
        try {
            // ── 1. Désérialisation JSON ───────────────────────────────────
            MesureCapteur mesure = MAPPER.readValue(record.value(), MesureCapteur.class);
            String ville   = mesure.getVille();
            double valeur  = mesure.getValeur();
            LocalDateTime ts = mesure.getTimestamp();

            // ── 2. Mise à jour de la fenêtre glissante ────────────────────
            // computeIfAbsent : crée une nouvelle Deque si la ville
            // apparaît pour la première fois.
            Deque<Double> fenetre = fenetresParVille
                    .computeIfAbsent(ville, k -> new ArrayDeque<>());

            fenetre.addLast(valeur); // ajouter la nouvelle valeur en queue

            // Si la fenêtre dépasse la taille max → retirer la plus ancienne
            if (fenetre.size() > TAILLE_FENETRE) {
                fenetre.pollFirst();
            }

            // ── 3. Calcul de la moyenne ───────────────────────────────────
            double moyenne = fenetre.stream()
                    .mapToDouble(Double::doubleValue)
                    .average()
                    .orElse(0.0);

            // AJOUT : Envoi de la valeur calculée à Prometheus
            tempMoyenneGauge.labels(ville).set(moyenne);

            // ── 4. Logging ────────────────────────────────────────────────
            // Format lisible : ville | valeur instantanée | moyenne fenêtrée
            logger.info(
                "📊 [{}] ville={} | valeur={}{} | moy.glissante({} pts)={} {}",
                ts,
                ville,
                String.format("%.2f", valeur),
                mesure.getUnite(),
                fenetre.size(),
                String.format("%.2f", moyenne),
                mesure.getUnite()
            );

            // Log de niveau DEBUG : détail de la fenêtre (visible si niveau=DEBUG dans log4j2.xml)
            logger.debug("   Fenêtre {} : {}", ville, fenetre);

        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            logger.error("❌ Impossible de désérialiser le message : {}", record.value(), e);
        }
    }
}
