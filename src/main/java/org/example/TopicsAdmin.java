package org.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.example.config.KafkaConfig;

public class TopicsAdmin {

    private static final int numPartitions = 3;

    private static final short replicationFactor = 1;

    private static final Logger logger = LoggerFactory.getLogger(TopicsAdmin.class);

    public static void main(String[] args) {
        createTopic(List.of("FIRST_TOPIC", "SECOND_TOPIC", "THIRD_TOPIC"));
        Set<String> topics = printAllTopics();
        topics.forEach(topic -> {
            deleteTopics(List.of(topic));
            printAllTopics();
        });
    }

    private static void createTopic(List<String> topicsName) {
        try (AdminClient adminClient = AdminClient.create(KafkaConfig.getAdminConfig())) {
            List<NewTopic> topics = topicsName.stream()
                    .map(t -> new NewTopic(t, numPartitions, replicationFactor))
                    .collect(Collectors.toList());
            CreateTopicsResult createTopicsResult = adminClient.createTopics(topics);
            KafkaFuture<Void> future = createTopicsResult.all();
            future.whenComplete((result, exception) -> {
                if (exception == null) {
                    logger.info("Топики из списка '{}' успешно созданы.", Arrays.toString(topicsName.toArray()));
                } else {
                    logger.error("Failed to create topic", exception);
                }
            });
            future.get();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static Set<String> printAllTopics() {
        try (AdminClient adminClient = AdminClient.create(KafkaConfig.getAdminConfig())) {
            Set<String> topicsName = adminClient.listTopics().names().get();
            logger.info("Получены топики: {}", topicsName);
            return topicsName;
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void deleteTopics(List<String> topicsName) {
        try (AdminClient adminClient = AdminClient.create(KafkaConfig.getAdminConfig())) {
            DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(topicsName);
            KafkaFuture<Void> future = deleteTopicsResult.all();
            future.whenComplete((result, exception) -> {
                if (exception == null) {
                    logger.info("Топики из списка '{}' успешно удалены.", Arrays.toString(topicsName.toArray()));
                } else {
                    logger.error("Failed to create topic", exception);
                }
            });
        }
    }

}
