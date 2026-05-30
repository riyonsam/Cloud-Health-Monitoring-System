package uk.ac.ed.inf.cw3.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import uk.ac.ed.inf.cw3.configuration.SystemEnvironment;

import java.time.Duration;
import java.util.*;

@Service
public class KafkaConsumerService {

    private final Map<String, Object> consumerProps;
    private final SystemEnvironment env;
    private final ObjectMapper mapper;

    public KafkaConsumerService(
            @Qualifier("kafkaConsumerProps") Map<String, Object> kafkaConsumerProps,
            SystemEnvironment env,
            ObjectMapper objectMapper) {
        this.consumerProps = kafkaConsumerProps;
        this.env = env;
        this.mapper = objectMapper;
    }

    // ----------------------------
    // Read recent health events from Kafka topic
    // Uses manual partition assignment + seekToBeginning to reliably
    // read all events without waiting for group rebalance
    // ----------------------------
    public List<Map<String, Object>> readRecentEvents(int maxMessages, long timeoutMs) {
        List<Map<String, Object>> events = new ArrayList<>();

        Map<String, Object> props = new HashMap<>(consumerProps);
        props.put("group.id", "health-stream-" + UUID.randomUUID());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {

            // Get all partitions for the topic
            List<PartitionInfo> partitions = consumer.partitionsFor(
                    env.getKafkaTopic(), Duration.ofMillis(3000));

            if (partitions == null || partitions.isEmpty()) {
                return List.of(Map.of("info",
                        "No partitions found for topic: " + env.getKafkaTopic()));
            }

            // Manually assign all partitions — avoids group rebalance delay
            List<TopicPartition> topicPartitions = partitions.stream()
                    .map(p -> new TopicPartition(p.topic(), p.partition()))
                    .toList();

            consumer.assign(topicPartitions);

            // Seek to beginning of all partitions
            consumer.seekToBeginning(topicPartitions);

            long deadline = System.currentTimeMillis() + timeoutMs;

            while (System.currentTimeMillis() < deadline && events.size() < maxMessages) {
                long remaining = deadline - System.currentTimeMillis();
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(Math.min(remaining, 500)));

                if (records.isEmpty()) break; // No more messages

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        Map<String, Object> event = mapper.readValue(record.value(), Map.class);
                        event.put("kafkaOffset",    record.offset());
                        event.put("kafkaPartition", record.partition());
                        event.put("kafkaTimestamp", record.timestamp());
                        events.add(event);
                    } catch (Exception e) {
                        // Skip unparseable records
                    }
                    if (events.size() >= maxMessages) break;
                }
            }

        } catch (Exception e) {
            events.add(Map.of("error",
                    "Failed to read from Kafka: " + e.getMessage()));
        }

        // Return most recent first
        Collections.reverse(events);
        return events;
    }
}
