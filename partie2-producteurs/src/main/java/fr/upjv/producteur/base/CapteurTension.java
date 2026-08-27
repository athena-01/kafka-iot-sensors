package fr.upjv.producteur.base;

import fr.upjv.producteur.commun.ProfilProduction;

import java.util.Random;

/**
 * Capteur de tension électrique simulé (réseau européen 230V).
 *
 * Design Pattern : TEMPLATE METHOD (sous-classe concrète)
 * ─────────────────────────────────────────────────────────
 * Implémente uniquement les deux méthodes abstraites de {@link Capteur} :
 *   • genererValeur() → tension en Volts autour de 230V
 *   • getUnite()      → "V"
 *
 * Modèle physique :
 * ─────────────────
 * La tension du réseau électrique européen est nominalement 230V.
 * La norme EN 50160 autorise des variations de ±10% (207V–253V).
 * On utilise une distribution gaussienne pour que les valeurs proches
 * de 230V soient plus probables — comportement réaliste d'un réseau stable.
 *
 * Plage effective : 207V – 253V (±10% de la valeur nominale)
 */
public class CapteurTension extends Capteur {

    private static final Random RANDOM           = new Random();
    private static final double TENSION_NOMINALE = 230.0; // Volts (réseau EU 50Hz)
    private static final double ECART_TYPE       =   7.0; // σ en Volts (~3% de 230V)
    private static final double TENSION_MIN      = 207.0; // Limite basse EN 50160
    private static final double TENSION_MAX      = 253.0; // Limite haute EN 50160

    /**
     * @param id     identifiant du capteur
     * @param profil profil de production (fréquence + bruit)
     */
    public CapteurTension(int id, ProfilProduction profil) {
        super(id, "tension", profil);
    }

    /**
     * {@inheritDoc}
     *
     * Génère une tension avec distribution gaussienne autour de 230V.
     * La tension est bornée entre 207V et 253V conformément à la norme.
     */
    @Override
    protected double genererValeur() {
        double tension = TENSION_NOMINALE + RANDOM.nextGaussian() * ECART_TYPE;
        // Bornage : respecte la norme EN 50160 sur les variations de tension
        return Math.max(TENSION_MIN, Math.min(TENSION_MAX, tension));
    }

    @Override
    protected String getUnite() {
        return "V";
    }
}
