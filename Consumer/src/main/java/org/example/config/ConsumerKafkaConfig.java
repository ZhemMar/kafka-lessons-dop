package org.example.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.example.deserializer.SymbolDeserializer;

import java.util.Properties;

public class ConsumerKafkaConfig {

    public static final String TOPIC_VOWELS = "vowels";
    public static final String TOPIC_CONSONANTS = "consonants";

    private static final String BOOTSTRAP_SERVERS = "localhost:9091, localhost:9092, localhost:9093";
    private static final String GROUP_ID = "my-consumer-group";

    private ConsumerKafkaConfig() { }

    public static Properties getConsumerConfig() {
        Properties properties = new Properties();
        /** Подключения к Kafka-брокеру BOOTSTRAP_SERVERS */
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        /** Идентификатор группы потребителей (consumer group ID) */
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);

        /** Использование LongDeserializer для десериализации ключей (Key) */
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());

        /** Использование PersonDeserializer для десериализации значений (Value) */
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SymbolDeserializer.class.getName());

        /** Управление поведением потребителя при первом подключении к топику или при потере сохраненного смещения:
         * - "earliest": начинает считывать сообщения с самого начала топика
         * - "latest": начинает считывать сообщения с самого последнего доступного смещения.
         * - "none": если нет сохраненного смещения, потребитель выбрасывает исключение.
         * - "error": потребитель выбрасывает исключение при отсутствии сохраненного смещения или если смещение находится
         * за пределами диапазона доступных смещений.
         * */
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }
}
