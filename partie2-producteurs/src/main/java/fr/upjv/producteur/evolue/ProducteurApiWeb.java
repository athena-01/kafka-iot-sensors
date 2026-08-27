package fr.upjv.producteur.evolue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import fr.upjv.producteur.commun.MesureCapteur;
import fr.upjv.producteur.commun.MessageSerialiser;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Producteur version évoluée — données réelles via API Web.
 *
 * ╔══════════════════════════════════════════════════════════════╗
 * ║  API utilisée : OpenWeatherMap (Current Weather Data)        ║
 * ║  URL : https://openweathermap.org/api                        ║
 * ║  Données : température réelle, humidité, vent               ║
 * ║  Fréquence : toutes les 60s (respecte le rate limit gratuit) ║
 * ╠══════════════════════════════════════════════════════════════╣
 * ║  Pourquoi OpenWeatherMap ?                                   ║
 * ║  • API gratuite (1000 appels/jour en free tier)             ║
 * ║  • Clé API simple à obtenir                                  ║
 * ║  • JSON bien documenté                                       ║
 * ║  • Données physiques réelles (température, tension → vent)   ║
 * ╠══════════════════════════════════════════════════════════════╣
 * ║  Design Patterns :                                           ║
 * ║  • même structure que la version de base (Virtual Threads)   ║
 * ║  • HttpClient Java 11+ (natif, pas de dépendance externe)   ║
 * ║  • Jackson pour parser la réponse JSON de l'API             ║
 * ╚══════════════════════════════════════════════════════════════╝
 *
 * ⚠️  CONFIGURATION REQUISE :
 * Définir la variable d'environnement OPENWEATHER_API_KEY
 * avec votre clé obtenue sur https://home.openweathermap.org/api_keys
 * (inscription gratuite, clé disponible en quelques minutes)
 */
public class ProducteurApiWeb {

    // ── Configuration — toutes les valeurs sont lisibles via variables d'env ──

    private static final String KAFKA_BROKER =
            System.getenv().getOrDefault("KAFKA_BROKER", "localhost:9092");

    private static final String TOPIC =
            System.getenv().getOrDefault("KAFKA_TOPIC", "capteurs-topic");

    /**
     * Clé API OpenWeatherMap.
     * Obtenir sur : https://home.openweathermap.org/api_keys (gratuit)
     * Définir via : export OPENWEATHER_API_KEY=votre_cle
     */
    private static final String API_KEY =
            System.getenv().getOrDefault("OPENWEATHER_API_KEY", "VOTRE_CLE_ICI");

    /**
     * Villes surveillées — chaque ville = 1 "capteur" virtuel.
     * Données météo réelles récupérées toutes les INTERVALLE_MS.
     */
    private static final List<String> VILLES = List.of(
            "Amiens,FR",
            "Paris,FR",
            "Lyon,FR"
    );

    /**
     * Intervalle entre deux appels API par ville.
     * 60 secondes : respecte le rate limit du plan gratuit (1000 appels/jour).
     * Calcul : 3 villes × 60s = 720 appels/12h (bien en dessous de la limite)
     */
    private static final long INTERVALLE_MS = 60_000L;

    private static final AtomicLong COMPTEUR_TOTAL = new AtomicLong(0);

    // ── Client HTTP natif Java 11+ ────────────────────────────────────────
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
    private static final ObjectMapper MAPPER    = new ObjectMapper();

    // ── Point d'entrée ────────────────────────────────────────────────────

    public static void main(String[] args) throws InterruptedException {

        afficherBanniere();
        verifierConfiguration();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,    KAFKA_BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props);
             ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {

            // Un Virtual Thread par ville surveillée
            for (int i = 0; i < VILLES.size(); i++) {
                final int    capteurId = i;
                final String ville     = VILLES.get(i);
                executor.submit(() -> boucleVille(capteurId, ville, producer));
            }

            surveillerIndefiniment();
        }
    }

    /**
     * Boucle de production pour une ville donnée.
     *
     * À chaque itération :
     *  1. Requête HTTP vers l'API OpenWeatherMap
     *  2. Parse du JSON de réponse avec Jackson
     *  3. Construction d'une MesureCapteur avec source="api-web"
     *  4. Envoi sur Kafka
     *  5. Attente de INTERVALLE_MS
     *
     * @param capteurId identifiant logique du capteur-ville
     * @param ville     nom de la ville (ex : "Amiens,FR")
     * @param producer  KafkaProducer partagé
     */
    private static void boucleVille(int capteurId,
                                     String ville,
                                     KafkaProducer<String, String> producer) {
        System.out.printf("🌍 Virtual Thread démarré → Ville : %s (ID %d)%n",
                ville, capteurId);

        while (!Thread.currentThread().isInterrupted()) {
            try {
                // ── 1. Appel API OpenWeatherMap ────────────────────────────
                String urlStr = buildUrl(ville);
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(urlStr))
                        .GET()
                        .build();

                HttpResponse<String> response =
                        HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() == 200) {
                    // ── 2. Parse JSON ──────────────────────────────────────
                    JsonNode root       = MAPPER.readTree(response.body());
                    double   temperature = root.path("main").path("temp").asDouble() - 273.15;
                    String   nomVille   = root.path("name").asText(ville);

                    // ── 3. Construire la mesure avec source="api-web" ──────
                    MesureCapteur mesure = new MesureCapteur(
                            capteurId,
                            "temperature",
                            Math.round(temperature * 100.0) / 100.0,
                            "°C",
                            "api-web:" + nomVille,   // source traçable pour la Personne 4
                            LocalDateTime.now()
                    );

                    // ── 4. Sérialiser et envoyer ───────────────────────────
                    String json = MessageSerialiser.toJson(mesure);
                    ProducerRecord<String, String> record =
                            new ProducerRecord<>(TOPIC, "temperature", json);

                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            System.err.printf("❌ [%s] Erreur Kafka : %s%n",
                                    ville, exception.getMessage());
                        } else {
                            long total = COMPTEUR_TOTAL.incrementAndGet();
                            System.out.printf(
                                    "✅ [%-15s] temp=%.1f°C | offset=%-6d | total=%d%n",
                                    nomVille, temperature, metadata.offset(), total);
                            System.out.println(json);
                        }
                    });

                } else if (response.statusCode() == 401) {
                    System.err.println("❌ Clé API invalide. Vérifiez OPENWEATHER_API_KEY.");
                    System.err.println("   Obtenez une clé sur : https://home.openweathermap.org/api_keys");
                } else {
                    System.err.printf("⚠️  API réponse %d pour %s%n",
                            response.statusCode(), ville);
                }

                // ── 5. Attente avant le prochain appel ────────────────────
                Thread.sleep(INTERVALLE_MS);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.out.printf("⛔ [%s] Arrêté proprement.%n", ville);
            } catch (Exception e) {
                System.err.printf("❌ [%s] Erreur : %s%n", ville, e.getMessage());
                // Attente courte avant de réessayer pour ne pas spammer l'API
                try { Thread.sleep(10_000); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * Construit l'URL de l'API OpenWeatherMap pour une ville donnée.
     * Format : https://api.openweathermap.org/data/2.5/weather?q=Amiens,FR&appid=KEY
     */
    private static String buildUrl(String ville) {
        return "https://api.openweathermap.org/data/2.5/weather?q="
                + ville.replace(" ", "%20")
                + "&appid=" + API_KEY;
    }

    /**
     * Vérifie que la clé API est configurée avant de démarrer.
     * Avertit l'utilisateur si elle est manquante sans planter brutalement.
     */
    private static void verifierConfiguration() {
        if ("VOTRE_CLE_ICI".equals(API_KEY) || API_KEY.isBlank()) {
            System.err.println("╔══════════════════════════════════════════════════════╗");
            System.err.println("║  ⚠️  CONFIGURATION MANQUANTE                         ║");
            System.err.println("║                                                      ║");
            System.err.println("║  Définissez la variable d'environnement :            ║");
            System.err.println("║    export OPENWEATHER_API_KEY=votre_cle_ici          ║");
            System.err.println("║                                                      ║");
            System.err.println("║  Obtenez une clé gratuite sur :                      ║");
            System.err.println("║    https://home.openweathermap.org/api_keys          ║");
            System.err.println("╚══════════════════════════════════════════════════════╝");
        }
    }

    private static void surveillerIndefiniment() throws InterruptedException {
        while (true) {
            Thread.sleep(60_000);
            System.out.printf(
                    "%n📊 [SURVEILLANCE] %d villes surveillées | %d messages envoyés%n%n",
                    VILLES.size(), COMPTEUR_TOTAL.get()
            );
        }
    }

    private static void afficherBanniere() {
        System.out.println("╔══════════════════════════════════════════════════════╗");
        System.out.println("║   PRODUCTEUR API WEB – Version Évoluée               ║");
        System.out.println("║   Master ISRI M1 2026 – Prog. Évènementielle         ║");
        System.out.println("╠══════════════════════════════════════════════════════╣");
        System.out.printf( "║   API         : OpenWeatherMap (free tier)           ║%n");
        System.out.printf( "║   Villes      : %-37s║%n", String.join(", ", VILLES));
        System.out.printf( "║   Intervalle  : %-37s║%n", INTERVALLE_MS / 1000 + "s par ville");
        System.out.printf( "║   Topic Kafka : %-37s║%n", TOPIC);
        System.out.println("╚══════════════════════════════════════════════════════╝");
    }
}
