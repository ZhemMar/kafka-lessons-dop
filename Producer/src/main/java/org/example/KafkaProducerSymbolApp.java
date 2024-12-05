package org.example;

import org.example.config.ProducerKafkaConfig;
import org.example.producer.domain.Symbol;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class KafkaProducerSymbolApp {

    private static final List<String> vowels = List.of("а", "у", "о", "и");
    private static final List<String> consonants = List.of("б", "в", "г", "д");

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerSymbolApp.class);

    public static void main(String[] args) {

        try (KafkaProducer<Long, Symbol> producer = new KafkaProducer<>(ProducerKafkaConfig.getProducerConfig())) {

            /** init transaction */
            producer.initTransactions();

            for (int i = 0; i < vowels.size(); i++) {
                try {
                    /** start transaction */
                    producer.beginTransaction();
                    Symbol symbol = createSymbol(i, vowels.get(i));
                    ProducerRecord<Long, Symbol> producerRecord = new ProducerRecord<>(ProducerKafkaConfig.TOPIC_VOWELS, ProducerKafkaConfig.PARTITION,
                            System.currentTimeMillis(), symbol.getId(), symbol);
                    RecordMetadata metadata = producer.send(producerRecord).get();
                    logger.info("Отправлено сообщение: key-{}, value-{}, offset: {}", i, symbol, metadata.offset());
                    /** commit transaction */
                    producer.commitTransaction();
                } catch (Exception e) {
                    /** abort transaction on error */
                    producer.abortTransaction();
                    logger.error("Ошибка при отправке сообщения в Kafka", e);
                    Thread.currentThread().interrupt();
                }
            }
            logger.info("Отправка завершена в топик 1");
            for (int i = 0; i < consonants.size(); i++) {
                try {
                    /** start transaction */
                    producer.beginTransaction();
                    Symbol symbol = createSymbol(i, consonants.get(i));
                    ProducerRecord<Long, Symbol> producerRecord = new ProducerRecord<>(ProducerKafkaConfig.TOPIC_CONSONANTS, ProducerKafkaConfig.PARTITION,
                            System.currentTimeMillis(), symbol.getId(), symbol);
                    RecordMetadata metadata = producer.send(producerRecord).get();
                    logger.info("Отправлено сообщение: key-{}, value-{}, offset: {}", i, symbol, metadata.offset());
                    /** commit transaction */
                    producer.commitTransaction();
                } catch (Exception e) {
                    /** abort transaction on error */
                    producer.abortTransaction();
                    logger.error("Ошибка при отправке сообщения в Kafka", e);
                    Thread.currentThread().interrupt();
                }
            }
            logger.info("Отправка завершена в топик 2");
        } catch (Exception e) {
            logger.error("Ошибка при отправке сообщений в Kafka", e);
        }
    }

    private static Symbol createSymbol(int index, String value) {
        String currentTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd-MM-yyyy-HH-mm-ss"));
        return new Symbol((long) index, value, "COLOR" + index, "TYPE" + index);
    }

}
