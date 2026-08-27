package fr.upjv.producteur.base;

import fr.upjv.producteur.commun.ProfilProduction;

import java.util.Random;

/**
 * Capteur de puissance électrique simulé.
 *
 * Design Pattern : TEMPLATE METHOD (sous-classe concrète)
 * ─────────────────────────────────────────────────────────
 * Simule la consommation électrique d'un équipement industriel.
 * La puissance varie selon des cycles de charge réalistes :
 * phase de démarrage (forte puissance), phase de régime nominal,
 * phase de veille (faible puissance).
 *
 * Plage : 0W – 5000W
 */
public class CapteurPuissance extends Capteur {

    private static final Random RANDOM        = new Random();
    private static final double PUISSANCE_MAX = 5_000.0; // Watts
    private static final double PUISSANCE_MIN =     0.0;

    /** Phase de fonctionnement courante (simule des cycles de charge). */
    private double puissanceActuelle = 1_000.0;

    public CapteurPuissance(int id, ProfilProduction profil) {
        super(id, "puissance", profil);
    }

    /**
     * Simule une variation de puissance avec des cycles de charge.
     * La puissance évolue progressivement avec des pics occasionnels
     * (démarrage d'équipement) et des creux (mise en veille).
     */
    @Override
    protected double genererValeur() {
        // Variation progressive ±200W avec pic aléatoire occasionnel
        double variation = (RANDOM.nextDouble() - 0.5) * 400.0;

        // 5% de chance d'un pic de démarrage
        if (RANDOM.nextDouble() < 0.05) {
            variation += RANDOM.nextDouble() * 2_000.0;
        }

        // 5% de chance d'une mise en veille
        if (RANDOM.nextDouble() < 0.05) {
            variation -= RANDOM.nextDouble() * 800.0;
        }

        puissanceActuelle += variation;
        puissanceActuelle  = Math.max(PUISSANCE_MIN,
                Math.min(PUISSANCE_MAX, puissanceActuelle));
        return puissanceActuelle;
    }

    @Override
    protected String getUnite() {
        return "W";
    }
}