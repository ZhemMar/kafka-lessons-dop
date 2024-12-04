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
            for (int i = 0; i < vowels.size(); i++) {
                Symbol symbol = createSymbol(i, vowels.get(i));
                ProducerRecord<Long, Symbol> producerRecord = new ProducerRecord<>(ProducerKafkaConfig.TOPIC_VOWELS, ProducerKafkaConfig.PARTITION,
                        System.currentTimeMillis(), symbol.getId(), symbol);
                sendMessage(producer, producerRecord, symbol);
            }
            logger.info("Отправка завершена в топик 1");
            for (int i = 0; i < consonants.size(); i++) {
                Symbol symbol = createSymbol(i, consonants.get(i));
                ProducerRecord<Long, Symbol> producerRecord = new ProducerRecord<>(ProducerKafkaConfig.TOPIC_CONSONANTS, ProducerKafkaConfig.PARTITION,
                        System.currentTimeMillis(), symbol.getId(), symbol);
                sendMessage(producer, producerRecord, symbol);
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

    private static void sendMessage(KafkaProducer<Long, Symbol> producer, ProducerRecord<Long, Symbol> producerRecord, Symbol symbol) {
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e != null) {
                    logger.error("Error sending message: {}", e.getMessage(), e);
                } else {
                    logger.info("Sent record: key={}, value={}, partition={}, offset={}",
                            symbol.getId(), symbol, recordMetadata.partition(), recordMetadata.offset());                        }
            }
        });
        logger.info("Отправлено сообщение: key-{}, value-{}", symbol.getId(), symbol);
    }

}
