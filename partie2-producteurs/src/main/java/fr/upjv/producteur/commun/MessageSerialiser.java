package fr.upjv.producteur.commun;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Utilitaire de sérialisation JSON via Jackson.
 *
 * Design Pattern : SINGLETON + UTILITY CLASS
 * ───────────────────────────────────────────
 * {@link ObjectMapper} est coûteux à créer et thread-safe une fois
 * configuré. On le crée une seule fois (singleton) et on expose
 * uniquement la méthode statique {@link #toJson(Object)}.
 *
 * Pourquoi Jackson plutôt que String.format ?
 * ─────────────────────────────────────────────
 * - Gère automatiquement l'échappement des caractères spéciaux
 * - Sérialise les dates Java (LocalDateTime) via JavaTimeModule
 * - Moins de code, moins d'erreurs, plus maintenable
 * - Standard industriel reconnu
 */
public final class MessageSerialiser {

    /**
     * Instance unique de ObjectMapper, configurée et thread-safe.
     * JavaTimeModule : permet de sérialiser LocalDateTime correctement.
     * WRITE_DATES_AS_TIMESTAMPS(false) : format ISO-8601 lisible.
     */
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .enable(SerializationFeature.INDENT_OUTPUT);

    // Classe utilitaire : pas d'instanciation
    private MessageSerialiser() {}

    /**
     * Sérialise n'importe quel objet Java en chaîne JSON.
     *
     * @param objet l'objet à sérialiser (ex : {@link MesureCapteur})
     * @return      chaîne JSON formatée et indentée
     * @throws RuntimeException si la sérialisation échoue (ne devrait pas arriver)
     */
    public static String toJson(Object objet) {
        try {
            return MAPPER.writeValueAsString(objet);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Erreur lors de la sérialisation JSON de : " + objet, e
            );
        }
    }

    /**
     * Désérialise une chaîne JSON vers un objet Java.
     * Utile pour les tests ou pour la Personne 4 (consommateurs).
     *
     * @param json  chaîne JSON à désérialiser
     * @param clazz classe cible
     * @return      instance de la classe cible
     */
    public static <T> T fromJson(String json, Class<T> clazz) {
        try {
            return MAPPER.readValue(json, clazz);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Erreur lors de la désérialisation JSON : " + json, e
            );
        }
    }
}
