package fr.upjv.producteur.base;

import fr.upjv.producteur.commun.MesureCapteur;
import fr.upjv.producteur.commun.MessageSerialiser;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Producteur principal — Version de base (données simulées).
 *
 * Architecture conforme au sujet :
 * 1 producteur Kafka, N capteurs simultanés via Virtual Threads.
 *
 * Virtual Threads (Java 21+) :
 * Contrairement aux platform threads (~1 Mo chacun), les Virtual
 * Threads ne consomment que quelques Ko — idéal pour simuler des
 * dizaines ou centaines de capteurs simultanés comme évoqué dans
 * le sujet.
 *
 * KafkaProducer est thread-safe : une seule instance partagée
 * suffit pour tous les Virtual Threads.
 */
public class ProducteurVirtuel {

    // ── Configuration — modifier ici pour changer le comportement ──
    private static final String KAFKA_BROKER =
            System.getenv().getOrDefault("KAFKA_BROKER", "localhost:9092");

    private static final String TOPIC =
            System.getenv().getOrDefault("KAFKA_TOPIC", "capteurs-topic");

    /** Nombre total de capteurs simulés simultanément. */
    private static final int NB_CAPTEURS = 5;

    /** Compteur global d'envois pour la surveillance. */
    private static final AtomicLong COMPTEUR_TOTAL = new AtomicLong(0);

    public static void main(String[] args) throws InterruptedException {

        afficherBanniere();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      KAFKA_BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.LINGER_MS_CONFIG,  "5");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        props.put(ProducerConfig.ACKS_CONFIG,       "1");

        List<Capteur> flotte = CapteurFactory.creerFlotte(NB_CAPTEURS);

        System.out.printf("%n📋 Flotte de %d capteurs créée :%n", NB_CAPTEURS);
        flotte.forEach(c -> System.out.printf(
                "   ↳ [Capteur %2d] %-12s %s%n",
                c.getId(), c.getType(), c.getProfil().getDescription()));

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props);
             ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {

            // Un Virtual Thread par capteur
            for (Capteur capteur : flotte) {
                executor.submit(() -> boucleProduction(capteur, producer));
            }

            // Thread principal : surveillance toutes les 15s
            while (true) {
                Thread.sleep(15_000);
                System.out.printf(
                        "%n📊 [SURVEILLANCE] %d capteurs actifs | %d messages envoyés%n%n",
                        NB_CAPTEURS, COMPTEUR_TOTAL.get());
            }
        }
    }

    /**
     * Boucle de production continue d'un capteur dans un Virtual Thread.
     *
     * À chaque itération :
     *  1. Produit une mesure via Template Method (Capteur.produireMesure())
     *  2. Sérialise en JSON via Jackson (MessageSerialiser)
     *  3. Envoie sur Kafka de façon asynchrone avec callback
     *  4. Attend selon l'intervalle du profil (non-bloquant sur VT)
     */
    private static void boucleProduction(Capteur capteur,
                                         KafkaProducer<String, String> producer) {
        System.out.printf("🟢 Virtual Thread démarré → Capteur %d (%s)%n",
                capteur.getId(), capteur.getType());

        while (true) {
            try {
                MesureCapteur mesure = capteur.produireMesure();
                String json          = MessageSerialiser.toJson(mesure);

                ProducerRecord<String, String> record =
                        new ProducerRecord<>(TOPIC, capteur.getType(), json);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.printf("❌ [Capteur %d] Erreur envoi : %s%n",
                                capteur.getId(), exception.getMessage());
                    } else {
                        long total = COMPTEUR_TOTAL.incrementAndGet();
                        System.out.printf(
                                "✅ [Capteur %2d | %-12s] offset=%-6d total=%d%n",
                                capteur.getId(), capteur.getType(),
                                metadata.offset(), total);
                        System.out.println(json);
                    }
                });

                Thread.sleep(capteur.getProfil().getIntervalleMs());

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.out.printf("⛔ [Capteur %d] Arrêté.%n", capteur.getId());
                return;
            } catch (Exception e) {
                System.err.printf("❌ [Capteur %d] Erreur inattendue : %s%n",
                        capteur.getId(), e.getMessage());
            }
        }
    }

    private static void afficherBanniere() {
        System.out.println("╔══════════════════════════════════════════════════════╗");
        System.out.println("║   PRODUCTEUR VIRTUEL – Version Base                  ║");
        System.out.println("║   Master ISRI M1 2026 – Prog. Évènementielle         ║");
        System.out.println("╠══════════════════════════════════════════════════════╣");
        System.out.printf( "║   Capteurs : %-40d║%n", NB_CAPTEURS);
        System.out.printf( "║   Topic    : %-40s║%n", TOPIC);
        System.out.printf( "║   Broker   : %-40s║%n", KAFKA_BROKER);
        System.out.println("╠══════════════════════════════════════════════════════╣");
        System.out.println("║   Virtual Threads (Java 21+) | Jackson | Kafka 4.2  ║");
        System.out.println("╚══════════════════════════════════════════════════════╝");
    }
}