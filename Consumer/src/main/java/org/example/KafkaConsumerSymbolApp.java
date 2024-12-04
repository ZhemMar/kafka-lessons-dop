package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.example.config.ConsumerKafkaConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.example.consumer.domain.Symbol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

/**
 * Webinar-02: Kafka consumer-service (прием экземпляров класса Person из topic2)
 * Использования метода consumer.poll().
 */
public class KafkaConsumerSymbolApp {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerSymbolApp.class);
    private static final Duration TEN_MILLISECONDS_INTERVAL = Duration.ofMillis(10);

    public static void main(String[] args) {
        KafkaConsumer<Long, Symbol> consumer = new KafkaConsumer<>(ConsumerKafkaConfig.getConsumerConfig());
        try (consumer) {
            consumer.subscribe(List.of(ConsumerKafkaConfig.TOPIC_VOWELS, ConsumerKafkaConfig.TOPIC_CONSONANTS));
            while (true) {
                ConsumerRecords<Long, Symbol> consumerRecords = consumer.poll(TEN_MILLISECONDS_INTERVAL);
                for (ConsumerRecord<Long, Symbol> cr : consumerRecords) {
                    logger.info("Received record: topic {}, key={}, value={}, partition={}, offset={}",
                            cr.topic(), cr.key(), cr.value(), cr.partition(), cr.offset());
                }
            }
        }
    }

}
