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

// Ce programme recupere les vraies donnees meteo sur Internet via l'API OpenWeatherMap
// et envoie les mesures de temperature en temps reel dans un serveur Kafka.
public class ProducteurApiWeb {

    // Adresse du serveur Kafka (utilise localhost par defaut si la variable d'environnement n'existe pas)
    private static final String KAFKA_BROKER =
            System.getenv().getOrDefault("KAFKA_BROKER", "localhost:9092");

    // Nom du sujet Kafka dans lequel on publie les messages
    private static final String TOPIC =
            System.getenv().getOrDefault("KAFKA_TOPIC", "capteurs-topic");

    // Cle d'authentification secrete pour interroger le service OpenWeatherMap
    private static final String API_KEY =
            System.getenv().getOrDefault("OPENWEATHER_API_KEY", "4727fe3a0de0156c59ae21c0e063d32f");

    // Liste des villes a surveiller (chaque ville se comporte comme un capteur independant)
    private static final List<String> VILLES = List.of(
            "Amiens,FR",
            "Paris,FR",
            "Lyon,FR"
    );

    // Temps d'attente de 60 secondes entre chaque requete pour respecter l'offre gratuite
    private static final long INTERVALLE_MS = 60_000L;

    // Compteur global securise pour suivre le nombre total de messages distribues
    private static final AtomicLong COMPTEUR_TOTAL = new AtomicLong(0);

    // Clients natifs pour effectuer les requetes HTTP et transformer le texte JSON en objets
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
    private static final ObjectMapper MAPPER    = new ObjectMapper();

    // Point d'entree de l'application
    public static void main(String[] args) throws InterruptedException {

        afficherBanniere();
        verifierConfiguration();

        // Definition des parametres de connexion pour le producteur Kafka
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,    KAFKA_BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        // Initialisation du producteur Kafka et du gestionnaire de threads legers (Virtual Threads)
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props);
             ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {

            // Attribution d'un thread leger dedie a la surveillance de chaque ville
            for (int i = 0; i < VILLES.size(); i++) {
                final int    capteurId = i;
                final String ville     = VILLES.get(i);
                executor.submit(() -> boucleVille(capteurId, ville, producer));
            }

            // Maintien du programme principal actif pour assurer le suivi
            surveillerIndefiniment();
        }
    }

    // Boucle d'execution executee en parallele pour collecter les donnees d'une ville
    private static void boucleVille(int capteurId,
                                    String ville,
                                    KafkaProducer<String, String> producer) {
        System.out.printf("Virtual Thread demarre -> Ville : %s (ID %d)%n", ville, capteurId);

        // La boucle tourne en continu tant que le thread n'est pas interrompu
        while (!Thread.currentThread().isInterrupted()) {
            try {
                // 1. Preparation et envoi de la requete HTTP vers l'API meteo
                String urlStr = buildUrl(ville);
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(urlStr))
                        .GET()
                        .build();

                HttpResponse<String> response =
                        HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());

                // Traitement de la reponse si le site renvoie un code de succes (200 OK)
                if (response.statusCode() == 200) {

                    // 2. Extraction des donnees de la reponse JSON
                    JsonNode root       = MAPPER.readTree(response.body());
                    // Conversion des Kelvins renvoyes par l'API en degres Celsius
                    double   temperature = root.path("main").path("temp").asDouble() - 273.15;
                    String   nomVille   = root.path("name").asText(ville);

                    // 3. Creation de l'objet mesure contenant toutes les informations utiles
                    MesureCapteur mesure = new MesureCapteur(
                            capteurId,
                            "temperature",
                            Math.round(temperature * 100.0) / 100.0, // Arrondi a deux chiffres apres la virgule
                            "°C",
                            "api-web:" + nomVille, // Identification claire de l'origine de la donnee
                            LocalDateTime.now()
                    );

                    // 4. Conversion de la mesure en texte JSON et envoi vers le serveur Kafka
                    String json = MessageSerialiser.toJson(mesure);
                    ProducerRecord<String, String> record =
                            new ProducerRecord<>(TOPIC, "temperature", json);

                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            // Affichage de l'erreur en cas d'echec de distribution Kafka
                            System.err.printf("Erreur Kafka pour [%s] : %s%n", ville, exception.getMessage());
                        } else {
                            // Incrémentation du compteur global et affichage du succes dans la console
                            long total = COMPTEUR_TOTAL.incrementAndGet();
                            System.out.printf("Suces [%-15s] temp=%.1f°C | offset=%-6d | total=%d%n",
                                    nomVille, temperature, metadata.offset(), total);
                            System.out.println(json);
                        }
                    });

                } else if (response.statusCode() == 401) {
                    // Signalement specifique si la cle secrete refuse l'acces
                    System.err.println("Cle API invalide. Verifiez la variable OPENWEATHER_API_KEY.");
                } else {
                    // Signalement pour les autres types d'erreurs (ville introuvable, serveur en panne)
                    System.err.printf("L'API a renvoye un code d'erreur %d pour %s%n", response.statusCode(), ville);
                }

                // 5. Mise en pause du thread pendant 60 secondes avant la prochaine collecte
                Thread.sleep(INTERVALLE_MS);

            } catch (InterruptedException e) {
                // Gestion de la fermeture propre du thread en cas d'arret de l'application
                Thread.currentThread().interrupt();
                System.out.printf("Arret propre de la surveillance pour [%s].%n", ville);
            } catch (Exception e) {
                // Securite pour eviter le plantage en cas de coupure reseau ou de panne de l'API
                System.err.printf("Erreur rencontree pour [%s] : %s%n", ville, e.getMessage());
                try {
                    // Attente de 10 secondes avant de reessayer pour preserver les ressources
                    Thread.sleep(10_000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    // Construction de l'adresse web de requete en encodant correctement les espaces du nom de la ville
    private static String buildUrl(String ville) {
        return "https://api.openweathermap.org/data/2.5/weather?q="
                + ville.replace(" ", "%20")
                + "&appid=" + API_KEY;
    }

    // Verification rudimentaire pour s'assurer que la cle secrete n'est pas vide
    private static void verifierConfiguration() {
        if (API_KEY.isBlank()) {
            System.err.println("Attention : La cle OPENWEATHER_API_KEY n'est pas renseignee dans l'environnement !");
        }
    }

    // Boucle de surveillance qui affiche un bilan d'activite toutes les minutes dans la console parent
    private static void surveillerIndefiniment() throws InterruptedException {
        while (true) {
            Thread.sleep(60_000);
            System.out.printf("%nSuivi : %d villes actives | %d messages distribues au total%n%n",
                    VILLES.size(), COMPTEUR_TOTAL.get());
        }
    }

    // Affichage textuel des parametres au lancement du programme
    private static void afficherBanniere() {
        System.out.println("--- Demarrage du producteur API Web ---");
        System.out.println("Villes suivies : " + String.join(", ", VILLES));
        System.out.println("Topic Kafka    : " + TOPIC);
        System.out.println("---------------------------------------");
    }
}