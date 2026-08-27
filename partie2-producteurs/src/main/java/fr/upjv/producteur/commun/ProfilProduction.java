package fr.upjv.producteur.commun;

/**
 * Profils de production disponibles pour un capteur.
 *
 * Design Pattern : STRATEGY (encapsulée dans une enum)
 * ──────────────────────────────────────────────────────
 * Chaque constante encapsule un comportement de production :
 * sa fréquence d'envoi et son niveau de bruit physique.
 * Le profil est injecté dans un Capteur à sa création, permettant
 * de varier indépendamment CE QUE le capteur mesure (sa classe)
 * et COMMENT il le mesure (son profil).
 *
 * Exemple :
 *   CapteurTemperature(id=1, ProfilProduction.LENT)   → 5s, bruit 1%
 *   CapteurTemperature(id=2, ProfilProduction.RAPIDE)  → 1s, bruit 5%
 *   CapteurTension(id=3,     ProfilProduction.TRES_RAPIDE) → 0.5s, bruit 10%
 */
public enum ProfilProduction {

    /**
     * Capteur lent : mesure toutes les 5 secondes, très précis.
     * Usage typique : température ambiante d'une salle.
     */
    LENT(5_000, 0.01, "Lent        (5s  – bruit 1%)"),

    /**
     * Capteur standard : mesure toutes les 2 secondes.
     * Usage typique : capteur d'usage général.
     */
    NORMAL(2_000, 0.02, "Normal      (2s  – bruit 2%)"),

    /**
     * Capteur rapide : mesure toutes les secondes, légèrement bruité.
     * Usage typique : tension réseau électrique.
     */
    RAPIDE(1_000, 0.05, "Rapide      (1s  – bruit 5%)"),

    /**
     * Capteur industriel : mesure 2 fois par seconde, bruit important.
     * Usage typique : capteur sur équipement critique.
     */
    TRES_RAPIDE(500, 0.10, "Très rapide (0.5s– bruit 10%)");

    /** Délai entre deux envois en millisecondes. */
    private final long intervalleMs;

    /** Facteur de bruit gaussien (0.0 = parfait, 0.10 = ±10%). */
    private final double niveauBruit;

    /** Description lisible pour les logs. */
    private final String description;

    ProfilProduction(long intervalleMs, double niveauBruit, String description) {
        this.intervalleMs = intervalleMs;
        this.niveauBruit  = niveauBruit;
        this.description  = description;
    }

    public long   getIntervalleMs() { return intervalleMs; }
    public double getNiveauBruit()  { return niveauBruit;  }
    public String getDescription()  { return description;  }
}
