package edu.supmti.kafka;

import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class WordProducer {
    public static void main(String[] args) {

        String topic = "WordsCount-Topic";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093,localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        Scanner scanner = new Scanner(System.in);
        System.out.println("Saisir du texte (Ctrl+C pour quitter) :");

        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            String[] words = line.toLowerCase().split("\\s+");

            for (String word : words) {
                ProducerRecord<String, String> record =
                        new ProducerRecord<>(topic, word);
                producer.send(record);
            }
        }

        producer.close();
    }
}
