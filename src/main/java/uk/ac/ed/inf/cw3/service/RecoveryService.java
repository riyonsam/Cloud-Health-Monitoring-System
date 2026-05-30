package uk.ac.ed.inf.cw3.service;

import com.rabbitmq.client.ConnectionFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import uk.ac.ed.inf.cw3.model.HealthEvent;
import uk.ac.ed.inf.cw3.model.ServiceStatus;

import java.util.HashMap;
import java.util.Map;

@Service
public class RecoveryService {

    private final S3Client s3Client;
    private final DynamoDbClient dynamoDbClient;
    private final JedisPool jedisPool;
    private final JdbcTemplate jdbcTemplate;
    private final ConnectionFactory rabbitConnectionFactory;
    private final KafkaProducer<String, String> kafkaProducer;

    public RecoveryService(
            S3Client s3Client,
            DynamoDbClient dynamoDbClient,
            JedisPool jedisPool,
            JdbcTemplate jdbcTemplate,
            @Qualifier("rabbitConnectionFactory") ConnectionFactory rabbitConnectionFactory,
            @Qualifier("kafkaProducer") KafkaProducer<String, String> kafkaProducer) {
        this.s3Client = s3Client;
        this.dynamoDbClient = dynamoDbClient;
        this.jedisPool = jedisPool;
        this.jdbcTemplate = jdbcTemplate;
        this.rabbitConnectionFactory = rabbitConnectionFactory;
        this.kafkaProducer = kafkaProducer;
    }

    // ----------------------------
    // Attempt recovery for a specific service
    // ----------------------------
    public HealthEvent attemptRecovery(String serviceName) {
        return switch (serviceName.toLowerCase()) {
            case "s3"       -> recoverS3();
            case "dynamodb" -> recoverDynamoDB();
            case "redis"    -> recoverRedis();
            case "postgres" -> recoverPostgres();
            case "rabbitmq" -> recoverRabbitMQ();
            case "kafka"    -> recoverKafka();
            default         -> new HealthEvent(serviceName, ServiceStatus.UNKNOWN,
                    "No recovery strategy defined for: " + serviceName, 0);
        };
    }

    // ----------------------------
    // Recovery strategies for all 6 services
    // ----------------------------

    private HealthEvent recoverS3() {
        long start = System.currentTimeMillis();
        HealthEvent event = new HealthEvent("S3", ServiceStatus.UNKNOWN,
                "Recovery attempted", 0);
        event.setRecoveryAttempted(true);
        try {
            s3Client.listBuckets(ListBucketsRequest.builder().build());
            event.setStatus(ServiceStatus.UP);
            event.setMessage("S3 recovery successful - connection restored");
            event.setRecoverySuccessful(true);
        } catch (Exception e) {
            event.setStatus(ServiceStatus.DOWN);
            event.setMessage("S3 recovery failed: " + e.getMessage());
            event.setRecoverySuccessful(false);
        }
        event.setResponseTimeMs(System.currentTimeMillis() - start);
        return event;
    }

    private HealthEvent recoverDynamoDB() {
        long start = System.currentTimeMillis();
        HealthEvent event = new HealthEvent("DynamoDB", ServiceStatus.UNKNOWN,
                "Recovery attempted", 0);
        event.setRecoveryAttempted(true);
        try {
            dynamoDbClient.listTables(ListTablesRequest.builder().limit(1).build());
            event.setStatus(ServiceStatus.UP);
            event.setMessage("DynamoDB recovery successful - connection restored");
            event.setRecoverySuccessful(true);
        } catch (Exception e) {
            event.setStatus(ServiceStatus.DOWN);
            event.setMessage("DynamoDB recovery failed: " + e.getMessage());
            event.setRecoverySuccessful(false);
        }
        event.setResponseTimeMs(System.currentTimeMillis() - start);
        return event;
    }

    private HealthEvent recoverRedis() {
        long start = System.currentTimeMillis();
        HealthEvent event = new HealthEvent("Redis", ServiceStatus.UNKNOWN,
                "Recovery attempted", 0);
        event.setRecoveryAttempted(true);
        try (Jedis jedis = jedisPool.getResource()) {
            String pong = jedis.ping();
            if ("PONG".equals(pong)) {
                event.setStatus(ServiceStatus.UP);
                event.setMessage("Redis recovery successful - PONG received");
                event.setRecoverySuccessful(true);
            } else {
                event.setStatus(ServiceStatus.DEGRADED);
                event.setMessage("Redis recovery partial - unexpected response: " + pong);
                event.setRecoverySuccessful(false);
            }
        } catch (Exception e) {
            event.setStatus(ServiceStatus.DOWN);
            event.setMessage("Redis recovery failed: " + e.getMessage());
            event.setRecoverySuccessful(false);
        }
        event.setResponseTimeMs(System.currentTimeMillis() - start);
        return event;
    }

    private HealthEvent recoverPostgres() {
        long start = System.currentTimeMillis();
        HealthEvent event = new HealthEvent("Postgres", ServiceStatus.UNKNOWN,
                "Recovery attempted", 0);
        event.setRecoveryAttempted(true);
        try {
            // Force a new connection attempt by executing a simple query
            // HikariCP will evict bad connections and establish new ones
            Integer result = jdbcTemplate.queryForObject("SELECT 1", Integer.class);
            if (result != null && result == 1) {
                event.setStatus(ServiceStatus.UP);
                event.setMessage("Postgres recovery successful - connection pool refreshed");
                event.setRecoverySuccessful(true);
            } else {
                event.setStatus(ServiceStatus.DEGRADED);
                event.setMessage("Postgres recovery partial - unexpected query result");
                event.setRecoverySuccessful(false);
            }
        } catch (Exception e) {
            event.setStatus(ServiceStatus.DOWN);
            event.setMessage("Postgres recovery failed: " + e.getMessage());
            event.setRecoverySuccessful(false);
        }
        event.setResponseTimeMs(System.currentTimeMillis() - start);
        return event;
    }

    private HealthEvent recoverRabbitMQ() {
        long start = System.currentTimeMillis();
        HealthEvent event = new HealthEvent("RabbitMQ", ServiceStatus.UNKNOWN,
                "Recovery attempted", 0);
        event.setRecoveryAttempted(true);
        try (var connection = rabbitConnectionFactory.newConnection()) {
            // Open a fresh connection and verify it is open
            if (connection.isOpen()) {
                // Also verify a channel can be created
                try (var channel = connection.createChannel()) {
                    if (channel.isOpen()) {
                        event.setStatus(ServiceStatus.UP);
                        event.setMessage("RabbitMQ recovery successful - new connection and channel opened");
                        event.setRecoverySuccessful(true);
                    } else {
                        event.setStatus(ServiceStatus.DEGRADED);
                        event.setMessage("RabbitMQ recovery partial - connection open but channel failed");
                        event.setRecoverySuccessful(false);
                    }
                }
            } else {
                event.setStatus(ServiceStatus.DOWN);
                event.setMessage("RabbitMQ recovery failed - connection not open");
                event.setRecoverySuccessful(false);
            }
        } catch (Exception e) {
            event.setStatus(ServiceStatus.DOWN);
            event.setMessage("RabbitMQ recovery failed: " + e.getMessage());
            event.setRecoverySuccessful(false);
        }
        event.setResponseTimeMs(System.currentTimeMillis() - start);
        return event;
    }

    private HealthEvent recoverKafka() {
        long start = System.currentTimeMillis();
        HealthEvent event = new HealthEvent("Kafka", ServiceStatus.UNKNOWN,
                "Recovery attempted", 0);
        event.setRecoveryAttempted(true);
        try {
            // Verify the producer can fetch partition metadata
            // This forces a connection to the broker
            var partitions = kafkaProducer.partitionsFor("health-events");
            if (!partitions.isEmpty()) {
                event.setStatus(ServiceStatus.UP);
                event.setMessage("Kafka recovery successful - broker reachable, "
                        + partitions.size() + " partition(s) found");
                event.setRecoverySuccessful(true);
            } else {
                event.setStatus(ServiceStatus.DEGRADED);
                event.setMessage("Kafka recovery partial - broker reachable but no partitions found");
                event.setRecoverySuccessful(false);
            }
        } catch (Exception e) {
            event.setStatus(ServiceStatus.DOWN);
            event.setMessage("Kafka recovery failed: " + e.getMessage());
            event.setRecoverySuccessful(false);
        }
        event.setResponseTimeMs(System.currentTimeMillis() - start);
        return event;
    }

    // ----------------------------
    // Get recovery strategies for all services
    // ----------------------------
    public Map<String, String> getRecoveryStrategies() {
        Map<String, String> strategies = new HashMap<>();
        strategies.put("S3",        "Re-verify S3 client by listing buckets");
        strategies.put("DynamoDB",  "Re-verify DynamoDB client by listing tables");
        strategies.put("Redis",     "Re-verify Redis pool by issuing PING command");
        strategies.put("Postgres",  "Force connection pool refresh via SELECT 1");
        strategies.put("RabbitMQ",  "Open fresh AMQP connection and verify channel");
        strategies.put("Kafka",     "Re-verify producer by fetching topic partition metadata");
        return strategies;
    }
}