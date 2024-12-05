package org.example.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.example.serializer.SymbolSerializer;

import java.util.Properties;

public class ProducerKafkaConfig {

    public static final String TOPIC_VOWELS = "vowels";
    public static final String TOPIC_CONSONANTS = "consonants";
    public static final int PARTITION = 0;

    private static final String BOOTSTRAP_SERVERS = "localhost:9091, localhost:9092, localhost:9093";

    private ProducerKafkaConfig() {
    }

    public static Properties getProducerConfig() {
        Properties properties = new Properties();

        /** Подключения к Kafka-брокеру BOOTSTRAP_SERVERS */
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        /** Сколько узлов должны подтвердить получение записи, прежде чем считать ее успешно записанной:
         *  - acks=0: продюсер не будет ждать подтверждений от брокера
         *  - acks=1: продюсер будет ждать подтверждения от лидера партиции, но не от всех реплик
         *  - acks=all продюсер будет ждать подтверждений от всех реплик (самая надежная настройка)
         */
        properties.put(ProducerConfig.ACKS_CONFIG, "all");

        /** Включение идемпотентности для продюсера */
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        /** Параметр используется для идентификации транзакций. Он назначает уникальный идентификатор транзакционному
         * продюсеру, позволяя ему выполнять и отслеживать транзакции.
         * При использовании транзакций producer должен иметь уникальный TRANSACTIONAL_ID, это Kafka отслеживать состояние
         * транзакций и гарантировать их атомарное выполнение.
         * Для работы с транзакциями Продюсер должен быть Идемпотентным, т.е. enable.idempotence=true
         */
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-transactional-id");

        /** Параметр конфигурации, определяющий максимальное время (в мс), в течение которого транзакция может оставаться
         * открытой. По умолчанию 60000 мс (1 минута)
         */
        properties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 600000);

        /** Использование LongSerializer для сериализации ключа (Key) */
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());

        /** Использование PersonSerializer для сериализации значения (Value) */
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SymbolSerializer.class.getName());

        return properties;
    }

}