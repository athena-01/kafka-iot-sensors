package fr.upjv.consommateur;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class Consommateur {

    public static void main(String[] args) {
        Properties consumerProperties = new Properties();

        // Ces propriétés sont équivalentes à ceux vues en cours.
        consumerProperties.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092"
        );
        consumerProperties.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );
        consumerProperties.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );
        consumerProperties.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "groupe-capteurs"
        );
        consumerProperties.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"
        );

        try (KafkaConsumer<String, String> monConsommateur =
                     new KafkaConsumer<>(consumerProperties)) {

            // On a en effet, un nouveau nom pour le topic.
            monConsommateur.subscribe(List.of("capteurs-topic"));

            System.out.println("Consommateur démarré...");

            while (true) {
                ConsumerRecords<String, String> lesRecordsRecu =
                        monConsommateur.poll(Duration.ofMillis(1000));

                if (!lesRecordsRecu.isEmpty()) {
                    for (ConsumerRecord<String, String> consumerRecord : lesRecordsRecu) {
                        System.out.println("-----------------------------------");
                        System.out.println("MESSAGE REÇU :");
                        System.out.println(consumerRecord.value());
                        System.out.println("-----------------------------------");
                    }
                } else {
                    System.out.println("Aucune donnée reçue...");
                }
            }
        }
    }
}