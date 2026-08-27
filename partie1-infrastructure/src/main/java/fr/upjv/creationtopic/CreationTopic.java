package fr.upjv.creationtopic;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class CreationTopic {

    public static void main(String[] args) {

        Properties properties = new Properties();

        // Ces propriétés sont équivalentes à ceux vues en cours.
        properties.put(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092"
        );

        try (AdminClient adminClient = AdminClient.create(properties)) {

            // On a un nouveau nom de topic.
            String nomDeTopic = "capteurs-topic";

            // Il contient une seule partition.
            // Suffisant à l'échelle de notre projet.
            int nbPartitions = 1;

            // Et un seul réplicat, pour la même raison.
            short nbReplicats = 1;

            NewTopic leTopic =
                    new NewTopic(
                            nomDeTopic,
                            nbPartitions,
                            nbReplicats
                    );
            // Création du topic.
            CreateTopicsResult createTopicsResult =
                    adminClient.createTopics(List.of(leTopic));

            // Attendre que la création soit terminée.
            createTopicsResult.all().get();

            System.out.println("Topic créé avec succès : " + nomDeTopic);

        } catch (ExecutionException e) {

            System.out.println("Le topic existe déjà ou erreur Kafka.");
            throw new RuntimeException(e);

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}