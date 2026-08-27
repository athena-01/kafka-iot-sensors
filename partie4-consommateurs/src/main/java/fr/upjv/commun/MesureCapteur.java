package fr.upjv.commun;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;

/**
 * Représente une mesure reçue depuis le topic Kafka.
 *
 * ─────────────────────────────────────────────────────────────
 * POURQUOI CETTE CLASSE EST ICI ?
 * ─────────────────────────────────────────────────────────────
 * La classe originale est dans partie2-producteurs.
 * Plutôt que de créer une dépendance entre modules Maven,
 * on recopie la classe ici (même package, même structure).
 * C'est une pratique courante dans les projets multi-modules
 * pour éviter les dépendances circulaires.
 *
 * ─────────────────────────────────────────────────────────────
 * FORMAT JSON ATTENDU (issu du ProducteurApiWeb) :
 * ─────────────────────────────────────────────────────────────
 * {
 *   "capteurId"  : 0,
 *   "type"       : "temperature",
 *   "valeur"     : 21.55,
 *   "unite"      : "°C",
 *   "source"     : "api-web:Amiens",
 *   "timestamp"  : "2026-06-01T12:26:43.910"
 * }
 *
 * ─────────────────────────────────────────────────────────────
 * LIEN AVEC LA BDD (init.sql) :
 * ─────────────────────────────────────────────────────────────
 * Table temperatures :
 *   ville      ← extrait de source (ex: "api-web:Amiens" → "Amiens")
 *   valeur     ← valeur
 *   horodatage ← timestamp
 */
public class MesureCapteur {

    @JsonProperty("capteurId")
    private int capteurId;

    @JsonProperty("type")
    private String type;

    @JsonProperty("valeur")
    private double valeur;

    @JsonProperty("unite")
    private String unite;

    @JsonProperty("source")
    private String source;

    @JsonProperty("timestamp")
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS")
    private LocalDateTime timestamp;

    // Constructeur vide requis par Jackson pour la désérialisation
    public MesureCapteur() {}

    public MesureCapteur(int capteurId, String type, double valeur,
                         String unite, String source, LocalDateTime timestamp) {
        this.capteurId = capteurId;
        this.type      = type;
        this.valeur    = valeur;
        this.unite     = unite;
        this.source    = source;
        this.timestamp = timestamp;
    }

    // ── Getters ────────────────────────────────────────────────────────────

    public int           getCapteurId() { return capteurId; }
    public String        getType()      { return type;      }
    public double        getValeur()    { return valeur;    }
    public String        getUnite()     { return unite;     }
    public String        getSource()    { return source;    }
    public LocalDateTime getTimestamp() { return timestamp; }

    /**
     * Extrait le nom de la ville depuis le champ source.
     *
     * Exemple : source = "api-web:Amiens" → retourne "Amiens"
     * Si le format est inattendu, retourne source tel quel.
     *
     * C'est cette valeur qui est insérée dans la colonne "ville"
     * de la table PostgreSQL.
     */
    public String getVille() {
        if (source != null && source.contains(":")) {
            return source.substring(source.indexOf(':') + 1);
        }
        return source;
    }

    @Override
    public String toString() {
        return "MesureCapteur{id=%d, type='%s', valeur=%.2f %s, source='%s', timestamp=%s}"
                .formatted(capteurId, type, valeur, unite, source, timestamp);
    }
}
