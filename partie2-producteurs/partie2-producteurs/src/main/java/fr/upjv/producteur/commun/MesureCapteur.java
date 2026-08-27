package fr.upjv.producteur.commun;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;

/**
 * Représente une mesure produite par un capteur physique.
 *
 * Design Pattern : VALUE OBJECT
 * ──────────────────────────────
 * Objet immuable portant les données d'une mesure unique.
 * Jackson sérialise automatiquement cet objet en JSON grâce
 * aux annotations {@link JsonProperty} et {@link JsonFormat}.
 *
 * Format JSON produit :
 * <pre>
 * {
 *   "capteurId"  : 3,
 *   "type"       : "temperature",
 *   "valeur"     : 23.47,
 *   "unite"      : "°C",
 *   "source"     : "simulation",
 *   "timestamp"  : "2026-05-25T14:32:01.123"
 * }
 * </pre>
 *
 * Le champ {@code source} permet à la Personne 4 (consommateurs)
 * de distinguer les données simulées ("simulation") des données
 * réelles issues d'une API Web ("api-web") sans changer le format.
 */
public class MesureCapteur {

    @JsonProperty("capteurId")
    private final int capteurId;

    @JsonProperty("type")
    private final String type;

    @JsonProperty("valeur")
    private final double valeur;

    @JsonProperty("unite")
    private final String unite;

    @JsonProperty("source")
    private final String source;

    @JsonProperty("timestamp")
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS")
    private final LocalDateTime timestamp;

    /**
     * Constructeur principal.
     *
     * @param capteurId  identifiant unique du capteur (ex : 0, 1, 2…)
     * @param type       type de mesure ("temperature", "tension"…)
     * @param valeur     valeur mesurée après application du bruit
     * @param unite      unité physique ("°C", "V"…)
     * @param source     origine de la donnée ("simulation" ou "api-web")
     * @param timestamp  horodatage de la mesure
     */
    public MesureCapteur(int capteurId,
                         String type,
                         double valeur,
                         String unite,
                         String source,
                         LocalDateTime timestamp) {
        this.capteurId = capteurId;
        this.type      = type;
        this.valeur    = valeur;
        this.unite     = unite;
        this.source    = source;
        this.timestamp = timestamp;
    }

    // ── Getters (requis par Jackson pour la sérialisation) ─────────

    public int           getCapteurId() { return capteurId; }
    public String        getType()      { return type;      }
    public double        getValeur()    { return valeur;    }
    public String        getUnite()     { return unite;     }
    public String        getSource()    { return source;    }
    public LocalDateTime getTimestamp() { return timestamp; }

    @Override
    public String toString() {
        return "MesureCapteur{id=%d, type='%s', valeur=%.2f %s, source='%s'}"
                .formatted(capteurId, type, valeur, unite, source);
    }
}
