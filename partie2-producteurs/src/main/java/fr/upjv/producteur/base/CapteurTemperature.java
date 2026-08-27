package fr.upjv.producteur.base;

import fr.upjv.producteur.commun.ProfilProduction;

import java.util.Random;

/**
 * Capteur de température simulé.
 *
 * Design Pattern : TEMPLATE METHOD (sous-classe concrète)
 * ─────────────────────────────────────────────────────────
 * Implémente uniquement les deux méthodes abstraites de {@link Capteur} :
 *   • genererValeur() → température en °C avec dérive progressive
 *   • getUnite()      → "°C"
 *
 * Modèle physique :
 * ─────────────────
 * On simule une dérive lente autour d'une valeur de base (22°C),
 * comme un vrai capteur thermique dans une pièce. La valeur oscille
 * progressivement plutôt que de sauter aléatoirement, ce qui rend
 * les graphiques Grafana plus réalistes et lisibles.
 *
 * Plage : 12°C – 32°C (TEMP_BASE ± AMPLITUDE)
 */
public class CapteurTemperature extends Capteur {

    private static final Random RANDOM      = new Random();
    private static final double TEMP_BASE   = 22.0;  // °C — référence ambiante
    private static final double AMPLITUDE   = 10.0;  // variation max ±10°C

    /**
     * État interne : dérive progressive de la température.
     * Commence à 0 (= valeur de base), oscille dans [-AMPLITUDE, +AMPLITUDE].
     */
    private double tendance = 0.0;

    /**
     * @param id     identifiant du capteur
     * @param profil profil de production (fréquence + bruit)
     */
    public CapteurTemperature(int id, ProfilProduction profil) {
        super(id, "temperature", profil);
    }

    /**
     * {@inheritDoc}
     *
     * Génère une température avec dérive progressive.
     * La tendance évolue de ±0.5°C à chaque appel et est bornée
     * pour éviter des valeurs physiquement aberrantes.
     */
    @Override
    protected double genererValeur() {
        // Dérive aléatoire bornée : simule une variation thermique lente
        tendance += (RANDOM.nextDouble() - 0.5) * 0.5;
        tendance  = Math.max(-AMPLITUDE, Math.min(AMPLITUDE, tendance));
        return TEMP_BASE + tendance;
    }

    @Override
    protected String getUnite() {
        return "°C";
    }
}
