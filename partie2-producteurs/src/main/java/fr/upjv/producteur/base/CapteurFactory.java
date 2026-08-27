package fr.upjv.producteur.base;

import fr.upjv.producteur.commun.ProfilProduction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Fabrique de capteurs simulés.
 *
 * Design Pattern : FACTORY METHOD
 * ─────────────────────────────────
 * Centralise la création des instances de Capteur.
 * L'appelant (ProducteurVirtuel) ne connaît pas les
 * sous-classes concrètes ; il demande simplement
 * une flotte via creerFlotte(n).
 *
 * Principe Ouvert/Fermé (OCP) :
 * Pour ajouter un nouveau type de capteur, on crée
 * la classe et on ajoute un case ici. Le reste
 * du code (ProducteurVirtuel) ne change pas.
 *
 * Répartition de la flotte :
 *   50% température  (profils Lent ou Normal)
 *   20% tension      (profils Rapide ou Très rapide)
 *   30% puissance    (profils Normal ou Rapide)
 */
public final class CapteurFactory {

    private static final Random RANDOM = new Random();

    private CapteurFactory() {}

    /**
     * Crée un capteur unique à partir de son type et de son profil.
     *
     * @param id     identifiant unique dans la flotte
     * @param type   "temperature", "tension" ou "puissance"
     * @param profil profil de production souhaité
     * @return       instance concrète de Capteur
     * @throws IllegalArgumentException si le type est inconnu
     */
    public static Capteur creer(int id, String type, ProfilProduction profil) {
        return switch (type.toLowerCase()) {
            case "temperature" -> new CapteurTemperature(id, profil);
            case "tension"     -> new CapteurTension(id, profil);
            case "puissance"   -> new CapteurPuissance(id, profil);
            default -> throw new IllegalArgumentException(
                    "Type inconnu : [" + type + "]. "
                            + "Types supportés : temperature, tension, puissance"
            );
        };
    }

    /**
     * Crée une flotte de N capteurs avec les 3 types et profils variés.
     *
     * @param nombreCapteurs nombre total de capteurs (minimum 1)
     * @return liste de capteurs prête à être démarrée
     */
    public static List<Capteur> creerFlotte(int nombreCapteurs) {
        if (nombreCapteurs < 1) {
            throw new IllegalArgumentException(
                    "La flotte doit contenir au moins 1 capteur."
            );
        }

        List<Capteur> flotte = new ArrayList<>(nombreCapteurs);
        int nbTemp      = (int) Math.round(nombreCapteurs * 0.5);
        int nbTension   = (int) Math.round(nombreCapteurs * 0.2);
        int nbPuissance = nombreCapteurs - nbTemp - nbTension;

        // Capteurs de température — profils lents ou normaux
        for (int i = 0; i < nbTemp; i++) {
            ProfilProduction profil = RANDOM.nextBoolean()
                    ? ProfilProduction.LENT
                    : ProfilProduction.NORMAL;
            flotte.add(creer(i, "temperature", profil));
        }

        // Capteurs de tension — profils rapides
        for (int i = nbTemp; i < nbTemp + nbTension; i++) {
            ProfilProduction profil = RANDOM.nextBoolean()
                    ? ProfilProduction.RAPIDE
                    : ProfilProduction.TRES_RAPIDE;
            flotte.add(creer(i, "tension", profil));
        }

        // Capteurs de puissance — profils normaux ou rapides
        for (int i = nbTemp + nbTension; i < nombreCapteurs; i++) {
            ProfilProduction profil = RANDOM.nextBoolean()
                    ? ProfilProduction.NORMAL
                    : ProfilProduction.RAPIDE;
            flotte.add(creer(i, "puissance", profil));
        }

        return flotte;
    }
}