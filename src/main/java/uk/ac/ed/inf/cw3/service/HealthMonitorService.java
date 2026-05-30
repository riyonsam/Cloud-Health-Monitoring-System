package uk.ac.ed.inf.cw3.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import uk.ac.ed.inf.cw3.configuration.SystemEnvironment;
import uk.ac.ed.inf.cw3.model.HealthEvent;
import uk.ac.ed.inf.cw3.model.ServiceStatus;

import java.util.ArrayList;
import java.util.List;

@Service
public class HealthMonitorService {

    private static final int AUTO_RECOVERY_THRESHOLD = 2;

    private final S3Client s3Client;
    private final DynamoDbClient dynamoDbClient;
    private final JdbcTemplate jdbcTemplate;
    private final JedisPool jedisPool;
    private final ConnectionFactory rabbitConnectionFactory;
    private final KafkaProducer<String, String> kafkaProducer;
    private final SystemEnvironment env;
    private final ObjectMapper mapper;

    // Setter-injected to avoid circular dependencies
    private RecoveryService recoveryService;
    private DynamoDbAnalyticsService analyticsService;
    private HealthEventProcessor eventProcessor;

    public HealthMonitorService(
            S3Client s3Client,
            DynamoDbClient dynamoDbClient,
            JdbcTemplate jdbcTemplate,
            JedisPool jedisPool,
            @Qualifier("rabbitConnectionFactory") ConnectionFactory rabbitConnectionFactory,
            @Qualifier("kafkaProducer") KafkaProducer<String, String> kafkaProducer,
            SystemEnvironment env,
            ObjectMapper objectMapper) {
        this.s3Client = s3Client;
        this.dynamoDbClient = dynamoDbClient;
        this.jdbcTemplate = jdbcTemplate;
        this.jedisPool = jedisPool;
        this.rabbitConnectionFactory = rabbitConnectionFactory;
        this.kafkaProducer = kafkaProducer;
        this.env = env;
        this.mapper = objectMapper;
    }

    public void setRecoveryService(RecoveryService recoveryService) {
        this.recoveryService = recoveryService;
    }

    public void setAnalyticsService(DynamoDbAnalyticsService analyticsService) {
        this.analyticsService = analyticsService;
    }

    public void setEventProcessor(HealthEventProcessor eventProcessor) {
        this.eventProcessor = eventProcessor;
    }

    // ----------------------------
    // Scheduled health check - runs every 30 seconds
    // ----------------------------
    @Scheduled(fixedDelayString = "${health.check.interval:30000}")
    public List<HealthEvent> runScheduledCheck() {
        return checkAllServices();
    }

    // ----------------------------
    // Check all services, track consecutive failures,
    // and trigger auto-recovery if threshold exceeded
    // ----------------------------
    public List<HealthEvent> checkAllServices() {
        List<HealthEvent> events = new ArrayList<>();

        events.add(checkS3());
        events.add(checkDynamoDB());
        events.add(checkPostgres());
        events.add(checkRedis());
        events.add(checkRabbitMQ());
        events.add(checkKafka());

        // Update consecutive failure counters and trigger auto-recovery
        try (Jedis jedis = jedisPool.getResource()) {
            for (HealthEvent event : events) {
                updateFailureCounter(jedis, event);
            }
        } catch (Exception e) {
            System.err.println("Failed to update failure counters: " + e.getMessage());
        }

        // Record events in DynamoDB analytics
        if (analyticsService != null) {
            for (HealthEvent event : events) {
                analyticsService.recordEvent(event);
            }
        }

        // Publish all events to Kafka
        for (HealthEvent event : events) {
            publishToKafka(event);
        }

        // Cache current status in Redis
        cacheCurrentStatus(events);

        return events;
    }

    // ----------------------------
    // Track consecutive failures and auto-recover
    // ----------------------------
    private void updateFailureCounter(Jedis jedis, HealthEvent event) {
        String counterKey = "health:failures:" + event.getServiceName();

        if (event.getStatus() == ServiceStatus.DOWN ||
                event.getStatus() == ServiceStatus.DEGRADED) {

            long failures = jedis.incr(counterKey);
            jedis.expire(counterKey, 3600);

            System.out.println("[HealthMonitor] " + event.getServiceName() +
                    " consecutive failures: " + failures);

            if (failures >= AUTO_RECOVERY_THRESHOLD && recoveryService != null) {
                System.out.println("[HealthMonitor] Auto-recovery triggered for " +
                        event.getServiceName() + " after " + failures + " consecutive failures");

                HealthEvent recoveryResult = recoveryService.attemptRecovery(
                        event.getServiceName());
                recoveryResult.setMessage("[AUTO-RECOVERY] " + recoveryResult.getMessage());

                if (recoveryResult.isRecoverySuccessful()) {
                    jedis.del(counterKey);
                    System.out.println("[HealthMonitor] Auto-recovery successful for " +
                            event.getServiceName());
                }

                // Publish recovery event to Kafka
                publishToKafka(recoveryResult);

                // Store in PostgreSQL history so it appears in the History tab
                if (eventProcessor != null) {
                    eventProcessor.processEvents(List.of(recoveryResult));
                }
            }

        } else if (event.getStatus() == ServiceStatus.UP) {
            // Check if there were previous failures before resetting
            String existingCount = jedis.get(counterKey);
            if (existingCount != null && Integer.parseInt(existingCount) > 0) {
                // Service recovered — store a success entry in history
                if (eventProcessor != null) {
                    HealthEvent recoverySuccess = new HealthEvent(
                            event.getServiceName(),
                            ServiceStatus.UP,
                            "[AUTO-RECOVERY] Service restored after " + existingCount + " consecutive failures",
                            event.getResponseTimeMs()
                    );
                    recoverySuccess.setRecoveryAttempted(true);
                    recoverySuccess.setRecoverySuccessful(true);
                    eventProcessor.processEvents(List.of(recoverySuccess));
                }
            }
            // Reset counter
            jedis.del(counterKey);
        }
    }

    // ----------------------------
    // Get consecutive failure count for a service
    // ----------------------------
    public int getConsecutiveFailures(String serviceName) {
        try (Jedis jedis = jedisPool.getResource()) {
            String val = jedis.get("health:failures:" + serviceName);
            return val != null ? Integer.parseInt(val) : 0;
        } catch (Exception e) {
            return 0;
        }
    }

    // ----------------------------
    // Individual service checkers
    // ----------------------------
    private HealthEvent checkS3() {
        long start = System.currentTimeMillis();
        try {
            s3Client.listBuckets(ListBucketsRequest.builder().build());
            return new HealthEvent("S3", ServiceStatus.UP,
                    "S3 is reachable", System.currentTimeMillis() - start);
        } catch (Exception e) {
            return new HealthEvent("S3", ServiceStatus.DOWN,
                    "S3 check failed: " + e.getMessage(),
                    System.currentTimeMillis() - start);
        }
    }

    private HealthEvent checkDynamoDB() {
        long start = System.currentTimeMillis();
        try {
            dynamoDbClient.listTables(ListTablesRequest.builder().limit(1).build());
            return new HealthEvent("DynamoDB", ServiceStatus.UP,
                    "DynamoDB is reachable", System.currentTimeMillis() - start);
        } catch (Exception e) {
            return new HealthEvent("DynamoDB", ServiceStatus.DOWN,
                    "DynamoDB check failed: " + e.getMessage(),
                    System.currentTimeMillis() - start);
        }
    }

    private HealthEvent checkPostgres() {
        long start = System.currentTimeMillis();
        try {
            jdbcTemplate.queryForObject("SELECT 1", Integer.class);
            return new HealthEvent("Postgres", ServiceStatus.UP,
                    "Postgres is reachable", System.currentTimeMillis() - start);
        } catch (Exception e) {
            return new HealthEvent("Postgres", ServiceStatus.DOWN,
                    "Postgres check failed: " + e.getMessage(),
                    System.currentTimeMillis() - start);
        }
    }

    private HealthEvent checkRedis() {
        long start = System.currentTimeMillis();
        try (Jedis jedis = jedisPool.getResource()) {
            String pong = jedis.ping();
            ServiceStatus status = "PONG".equals(pong) ? ServiceStatus.UP : ServiceStatus.DEGRADED;
            return new HealthEvent("Redis", status,
                    "Redis ping: " + pong, System.currentTimeMillis() - start);
        } catch (Exception e) {
            return new HealthEvent("Redis", ServiceStatus.DOWN,
                    "Redis check failed: " + e.getMessage(),
                    System.currentTimeMillis() - start);
        }
    }

    private HealthEvent checkRabbitMQ() {
        long start = System.currentTimeMillis();
        try (var connection = rabbitConnectionFactory.newConnection()) {
            boolean isOpen = connection.isOpen();
            ServiceStatus status = isOpen ? ServiceStatus.UP : ServiceStatus.DEGRADED;
            return new HealthEvent("RabbitMQ", status,
                    "RabbitMQ connection: " + (isOpen ? "open" : "closed"),
                    System.currentTimeMillis() - start);
        } catch (Exception e) {
            return new HealthEvent("RabbitMQ", ServiceStatus.DOWN,
                    "RabbitMQ check failed: " + e.getMessage(),
                    System.currentTimeMillis() - start);
        }
    }

    private HealthEvent checkKafka() {
        long start = System.currentTimeMillis();
        try {
            kafkaProducer.partitionsFor("health-events");
            return new HealthEvent("Kafka", ServiceStatus.UP,
                    "Kafka is reachable", System.currentTimeMillis() - start);
        } catch (Exception e) {
            return new HealthEvent("Kafka", ServiceStatus.DOWN,
                    "Kafka check failed: " + e.getMessage(),
                    System.currentTimeMillis() - start);
        }
    }

    // ----------------------------
    // Publish health event to Kafka topic
    // ----------------------------
    private void publishToKafka(HealthEvent event) {
        try {
            String json = mapper.writeValueAsString(event);
            kafkaProducer.send(new ProducerRecord<>(env.getKafkaTopic(),
                    event.getServiceName(), json));
            kafkaProducer.flush();
        } catch (Exception e) {
            System.err.println("Failed to publish health event to Kafka: " + e.getMessage());
        }
    }

    // ----------------------------
    // Cache current status in Redis for fast lookups
    // ----------------------------
    private void cacheCurrentStatus(List<HealthEvent> events) {
        try (Jedis jedis = jedisPool.getResource()) {
            for (HealthEvent event : events) {
                String key = "health:status:" + event.getServiceName();
                String value = event.getStatus().name() + "|" +
                        event.getMessage() + "|" +
                        event.getResponseTimeMs() + "ms|" +
                        event.getTimestamp().toString();
                jedis.setex(key, 60, value);
            }
        } catch (Exception e) {
            System.err.println("Failed to cache health status: " + e.getMessage());
        }
    }
}