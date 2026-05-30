package uk.ac.ed.inf.cw3.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import uk.ac.ed.inf.cw3.configuration.SystemEnvironment;
import uk.ac.ed.inf.cw3.model.HealthEvent;
import uk.ac.ed.inf.cw3.model.ServiceStatus;
import uk.ac.ed.inf.cw3.service.*;
import java.util.stream.Collectors;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

@RestController
@RequestMapping("/api/v1/health")
public class HealthController {

    private final HealthMonitorService monitorService;
    private final HealthEventProcessor eventProcessor;
    private final RecoveryService recoveryService;
    private final StatsService statsService;
    private final ReportService reportService;
    private final KafkaConsumerService kafkaConsumerService;
    private final DynamoDbAnalyticsService analyticsService;
    private final JedisPool jedisPool;
    private final ConnectionFactory rabbitConnectionFactory;
    private final SystemEnvironment env;
    private final ObjectMapper mapper;

    public HealthController(
            HealthMonitorService monitorService,
            HealthEventProcessor eventProcessor,
            RecoveryService recoveryService,
            StatsService statsService,
            ReportService reportService,
            KafkaConsumerService kafkaConsumerService,
            DynamoDbAnalyticsService analyticsService,
            JedisPool jedisPool,
            @Qualifier("rabbitConnectionFactory") ConnectionFactory rabbitConnectionFactory,
            SystemEnvironment env,
            ObjectMapper objectMapper) {
        this.monitorService   = monitorService;
        this.eventProcessor   = eventProcessor;
        this.recoveryService  = recoveryService;
        this.statsService     = statsService;
        this.reportService        = reportService;
        this.kafkaConsumerService = kafkaConsumerService;
        this.analyticsService     = analyticsService;
        this.jedisPool        = jedisPool;
        this.rabbitConnectionFactory = rabbitConnectionFactory;
        this.env    = env;
        this.mapper = objectMapper;
    }

    @PostMapping("/check")
    public ResponseEntity<List<HealthEvent>> triggerCheck() {
        List<HealthEvent> events = monitorService.checkAllServices();
        eventProcessor.processEvents(events);
        return ResponseEntity.ok(events);
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        List<String> services = List.of("S3", "DynamoDB", "Postgres",
                "Redis", "RabbitMQ", "Kafka");

        try (Jedis jedis = jedisPool.getResource()) {
            for (String service : services) {
                String cached = jedis.get("health:status:" + service);
                int failures  = monitorService.getConsecutiveFailures(service);

                if (cached != null) {
                    String[] parts = cached.split("\\|");
                    Map<String, Object> svc = new LinkedHashMap<>();
                    svc.put("status",              parts[0]);
                    svc.put("message",             parts.length > 1 ? parts[1] : "");
                    svc.put("responseTime",        parts.length > 2 ? parts[2] : "");
                    svc.put("checkedAt",           parts.length > 3 ? parts[3] : "");
                    svc.put("consecutiveFailures", failures);
                    status.put(service, svc);
                } else {
                    status.put(service, Map.of(
                            "status", "UNKNOWN",
                            "message", "No recent check data",
                            "consecutiveFailures", failures));
                }
            }
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of("error", e.getMessage()));
        }
        return ResponseEntity.ok(status);
    }

    @GetMapping("/history")
    public ResponseEntity<List<Map<String, Object>>> getHistory(
            @RequestParam(required = false) String service,
            @RequestParam(defaultValue = "50") int limit) {
        return ResponseEntity.ok(eventProcessor.getHistory(service, limit));
    }

    @GetMapping("/alerts")
    public ResponseEntity<List<Map<String, Object>>> getAlerts(
            @RequestParam(defaultValue = "100") int maxMessages) {
        List<Map<String, Object>> alerts = new ArrayList<>();
        try (Connection conn = rabbitConnectionFactory.newConnection();
             Channel channel = conn.createChannel()) {
            channel.queueDeclare(env.getAlertQueue(), true, false, false, null);
            for (int i = 0; i < maxMessages; i++) {
                GetResponse r = channel.basicGet(env.getAlertQueue(), true);
                if (r == null) break;
                alerts.add(mapper.readValue(
                        new String(r.getBody(), StandardCharsets.UTF_8), Map.class));
            }
        } catch (Exception e) {
            return ResponseEntity.ok(List.of(Map.of("error", e.getMessage())));
        }
        return ResponseEntity.ok(alerts);
    }

    @PostMapping("/recover/{service}")
    public ResponseEntity<HealthEvent> triggerRecovery(@PathVariable String service) {
        HealthEvent result = recoveryService.attemptRecovery(service);
        eventProcessor.processEvents(List.of(result));
        return ResponseEntity.ok(result);
    }

    @GetMapping("/strategies")
    public ResponseEntity<Map<String, String>> getStrategies() {
        return ResponseEntity.ok(recoveryService.getRecoveryStrategies());
    }

    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getStats(
            @RequestParam(required = false) String service) {
        if (service != null && !service.isBlank()) {
            return ResponseEntity.ok(Map.of(service,
                    statsService.getServiceStats(service)));
        }
        return ResponseEntity.ok(statsService.getAllStats());
    }

    @PostMapping("/simulate/down/{service}")
    public ResponseEntity<Map<String, Object>> simulateDown(@PathVariable String service) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.setex("health:status:" + service, 60,
                    "DOWN|Simulated failure for demo purposes|0ms|" + Instant.now());
            HealthEvent event = new HealthEvent(service, ServiceStatus.DOWN,
                    "Simulated failure for demo purposes", 0);
            eventProcessor.processEvents(List.of(event));
            return ResponseEntity.ok(Map.of(
                    "service", service, "status", "DOWN",
                    "message", "Service " + service + " marked as DOWN for demo",
                    "expiresInSeconds", 60));
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/simulate/up/{service}")
    public ResponseEntity<Map<String, Object>> simulateUp(@PathVariable String service) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del("health:status:" + service);
            jedis.del("health:failures:" + service);
            return ResponseEntity.ok(Map.of(
                    "service", service,
                    "message", "Simulated failure cleared for " + service));
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of("error", e.getMessage()));
        }
    }

    // ----------------------------
    // POST /api/v1/health/report
    // Generate and save health report to S3
    // ----------------------------
    @PostMapping("/report")
    public ResponseEntity<Map<String, Object>> generateReport() {
        try {
            return ResponseEntity.ok(reportService.generateAndSaveReport());
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of("error", e.getMessage()));
        }
    }

    // ----------------------------
    // GET /api/v1/health/report
    // List all saved reports in S3
    // ----------------------------
    @GetMapping("/report")
    public ResponseEntity<List<Map<String, Object>>> listReports() {
        return ResponseEntity.ok(reportService.listReports());
    }

    // ----------------------------
    // GET /api/v1/health/report/{key}
    // Fetch a specific report from S3
    // ----------------------------
    @GetMapping("/report/{key}")
    public ResponseEntity<Map<String, Object>> getReport(@PathVariable String key) {
        try {
            return ResponseEntity.ok(reportService.getReport(key));
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of("error", e.getMessage()));
        }
    }

    // ----------------------------
    // GET /api/v1/health/stream
    // Read recent health events directly from Kafka topic
    // ----------------------------
    @GetMapping("/stream")
    public ResponseEntity<List<Map<String, Object>>> getStream(
            @RequestParam(defaultValue = "50") int limit,
            @RequestParam(defaultValue = "3000") long timeoutMs) {
        return ResponseEntity.ok(
                kafkaConsumerService.readRecentEvents(limit, timeoutMs));
    }

    // ----------------------------
    // GET /api/v1/health/analytics
    // Get DynamoDB analytics for all services or a specific one
    // ----------------------------
    @GetMapping("/analytics")
    public ResponseEntity<Object> getAnalytics(
            @RequestParam(required = false) String service,
            @RequestParam(defaultValue = "20") int limit) {
        if (service != null && !service.isBlank()) {
            return ResponseEntity.ok(analyticsService.getAnalytics(service, limit));
        }
        return ResponseEntity.ok(analyticsService.getAnalyticsSummary());
    }

    // ----------------------------
    // POST /api/v1/health/analytics/snapshot
    // Manually trigger DynamoDB analytics snapshot
    // ----------------------------
    @PostMapping("/analytics/snapshot")
    public ResponseEntity<Map<String, Object>> triggerSnapshot() {
        analyticsService.storeHourlyStats();
        return ResponseEntity.ok(Map.of(
                "message", "Analytics snapshot stored in DynamoDB",
                "table",   "health_analytics",
                "timestamp", java.time.Instant.now().toString()
        ));
    }

}