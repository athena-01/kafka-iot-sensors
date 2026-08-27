package fr.upjv.producteur.commun;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Ce fichier sert d'outil pour convertir les données de nos capteurs en texte au format JSON,
 * Kafka ne sait envoyer que du texte simple, donc on utilise la bibliothèque
 * Jackson pour transformer automatiquement nos objets Java en texte lisible.
 */
public final class MessageSerialiser {

    // On crée une seule fois le moteur de conversion (MAPPER) pour tout le projet car il est lourd à charger.
    private static final ObjectMapper MAPPER = new ObjectMapper()
            // On lui ajoute un module pour qu'il sache lire et écrire les heures et les dates Java correctement.
            .registerModule(new JavaTimeModule())
            // On désactive le format informatique (qui écrit les dates sous forme de grands nombres de millisecondes).
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            // On active l'alignement du texte pour que le JSON soit joli et facile à lire à l'écran.
            .enable(SerializationFeature.INDENT_OUTPUT);

    // Constructeur privé : empêche de créer un objet de cette classe car elle ne contient que des outils partagés.
    private MessageSerialiser() {}

    /**
     * Cette méthode prend un objet Java (comme une mesure de capteur) et la transforme en texte JSON.
     */
    public static String toJson(Object objet) {
        try {
            // Le moteur transforme l'objet reçu en une ligne de texte et la renvoie.
            return MAPPER.writeValueAsString(objet);
        } catch (Exception e) {
            // Si le moteur bloque (ce qui est rare), on attrape le problème et on affiche un message clair.
            throw new RuntimeException(
                    "Impossible de transformer cet objet en texte JSON : " + objet, e
            );
        }
    }

    /**
     * Cette méthode fait l'inverse : elle prend un texte JSON et le retransforme en un vrai objet Java. Utile pour la version producteur api web.
     */
    public static <T> T fromJson(String json, Class<T> clazz) {
        try {
            // Le moteur lit le texte JSON et reconstruit l'objet Java demandé.
            return MAPPER.readValue(json, clazz);
        } catch (Exception e) {
            // Si le texte JSON est mal écrit ou corrompu, on signale l'erreur proprement.
            throw new RuntimeException(
                    "Impossible de transformer ce texte JSON en objet Java : " + json, e
            );
        }
    }
}