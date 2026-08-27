package fr.upjv.producteur.base;

import fr.upjv.producteur.commun.MesureCapteur;
import fr.upjv.producteur.commun.ProfilProduction;

import java.time.LocalDateTime;

/**
 * Classe abstraite représentant un capteur physique simulé.
 *
 * ╔══════════════════════════════════════════════════════╗
 * ║  Design Pattern : TEMPLATE METHOD                    ║
 * ╠══════════════════════════════════════════════════════╣
 * ║  Cette classe définit le SQUELETTE de l'algorithme   ║
 * ║  de production d'une mesure :                        ║
 * ║    1. Générer la valeur brute    → genererValeur()   ║
 * ║    2. Appliquer le bruit         → appliquerBruit()  ║
 * ║    3. Construire l'objet mesure  → produireMesure()  ║
 * ║                                                      ║
 * ║  Seule genererValeur() est abstraite. Les sous-      ║
 * ║  classes (CapteurTemperature, CapteurTension)         ║
 * ║  n'implémentent que CETTE méthode.                   ║
 * ║                                                      ║
 * ║  Avantage : le bruit, l'horodatage et la             ║
 * ║  construction de MesureCapteur sont écrits           ║
 * ║  une seule fois ici, jamais dupliqués.               ║
 * ╚══════════════════════════════════════════════════════╝
 */
public abstract class Capteur {

    private final int             id;
    private final String          type;
    private final ProfilProduction profil;

    /**
     * @param id     identifiant unique du capteur dans la flotte
     * @param type   libellé du type de mesure ("temperature", "tension")
     * @param profil profil de production (fréquence + niveau de bruit)
     */
    protected Capteur(int id, String type, ProfilProduction profil) {
        this.id     = id;
        this.type   = type;
        this.profil = profil;
    }

    // ── Méthodes abstraites : contrat imposé aux sous-classes ───────

    /**
     * Génère la valeur physique brute du capteur (sans bruit).
     * Chaque sous-classe implémente son propre modèle physique.
     *
     * @return valeur mesurée en unité physique (°C, V…)
     */
    protected abstract double genererValeur();

    /**
     * Retourne l'unité physique de la mesure.
     *
     * @return unité (ex : "°C", "V")
     */
    protected abstract String getUnite();

    // ── Template Method : algorithme complet de production ──────────

    /**
     * Produit une mesure complète prête à être sérialisée et envoyée.
     *
     * Étapes (fixes, définies ici) :
     *  1. Appel polymorphe à genererValeur() → valeur brute
     *  2. Application du bruit gaussien      → valeur réaliste
     *  3. Construction de MesureCapteur      → objet sérialisable
     *
     * Cette méthode est {@code final} : les sous-classes ne peuvent
     * pas modifier l'ordre des étapes.
     *
     * @return objet MesureCapteur prêt pour la sérialisation Jackson
     */
    public final MesureCapteur produireMesure() {
        double valeurBrute    = genererValeur();
        double valeurBruitee  = appliquerBruit(valeurBrute);
        double valeurArrondie = Math.round(valeurBruitee * 100.0) / 100.0;

        return new MesureCapteur(
                id,
                type,
                valeurArrondie,
                getUnite(),
                "simulation",          // source = simulation (vs "api-web" en version évoluée)
                LocalDateTime.now()
        );
    }

    /**
     * Applique un bruit gaussien à la valeur brute.
     *
     * Le bruit est proportionnel à la valeur et au niveau de bruit
     * du profil, simulant les imprécisions d'un vrai capteur physique.
     * On utilise Math.random() centré sur 0 pour un bruit symétrique.
     *
     * @param valeur valeur brute sans bruit
     * @return valeur avec bruit physique simulé
     */
    private double appliquerBruit(double valeur) {
        double bruit = (Math.random() - 0.5) * 2.0
                       * profil.getNiveauBruit()
                       * Math.abs(valeur);
        return valeur + bruit;
    }

    // ── Accesseurs ──────────────────────────────────────────────────

    public int             getId()     { return id;     }
    public String          getType()   { return type;   }
    public ProfilProduction getProfil() { return profil; }
}
