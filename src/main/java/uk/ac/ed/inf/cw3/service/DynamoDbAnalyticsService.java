package uk.ac.ed.inf.cw3.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import uk.ac.ed.inf.cw3.model.HealthEvent;
import uk.ac.ed.inf.cw3.model.ServiceStatus;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Service
public class DynamoDbAnalyticsService {

    private static final String TABLE_NAME = "health_analytics";
    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.of("UTC"));
    private static final DateTimeFormatter HOUR_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd_HH").withZone(ZoneId.of("UTC"));

    private final DynamoDbClient dynamoDb;
    private final StatsService statsService;
    private final ObjectMapper mapper;

    public DynamoDbAnalyticsService(
            DynamoDbClient dynamoDb,
            StatsService statsService,
            ObjectMapper objectMapper) {
        this.dynamoDb = dynamoDb;
        this.statsService = statsService;
        this.mapper = objectMapper;
    }

    // ----------------------------
    // Store a health event as an analytics record in DynamoDB
    // Called every time a health check runs
    // ----------------------------
    public void recordEvent(HealthEvent event) {
        try {
            ensureTableExists();

            String hourKey  = HOUR_FORMATTER.format(Instant.now());
            String pk       = event.getServiceName() + "#" + hourKey;

            // Try to update existing record for this service+hour
            // If not exists, create new one
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("pk",          av(pk));
            item.put("serviceName", av(event.getServiceName()));
            item.put("hourKey",     av(hourKey));
            item.put("dateKey",     av(DATE_FORMATTER.format(Instant.now())));
            item.put("lastStatus",  av(event.getStatus().name()));
            item.put("lastChecked", av(event.getTimestamp().toString()));
            item.put("responseMs",  avn(String.valueOf(event.getResponseTimeMs())));

            dynamoDb.putItem(PutItemRequest.builder()
                    .tableName(TABLE_NAME)
                    .item(item)
                    .build());

        } catch (Exception e) {
            System.err.println("[DynamoDB Analytics] Failed to record event: " + e.getMessage());
        }
    }

    // ----------------------------
    // Store hourly aggregated stats - runs every hour
    // ----------------------------
    @Scheduled(fixedDelay = 3600000)
    public void storeHourlyStats() {
        try {
            ensureTableExists();
            List<String> services = List.of("S3", "DynamoDB", "Postgres",
                    "Redis", "RabbitMQ", "Kafka");
            String hourKey = HOUR_FORMATTER.format(Instant.now());

            for (String service : services) {
                Map<String, Object> stats = statsService.getServiceStats(service);
                String pk = "HOURLY#" + service + "#" + hourKey;

                Map<String, AttributeValue> item = new HashMap<>();
                item.put("pk",             av(pk));
                item.put("serviceName",    av(service));
                item.put("hourKey",        av(hourKey));
                item.put("type",           av("HOURLY_AGGREGATE"));
                item.put("uptimePercent",  avn(String.valueOf(stats.get("uptimePercent"))));
                item.put("totalChecks",    avn(String.valueOf(stats.get("totalChecks"))));
                item.put("upCount",        avn(String.valueOf(stats.get("upCount"))));
                item.put("downCount",      avn(String.valueOf(stats.get("downCount"))));
                item.put("avgResponseMs",  avn(String.valueOf(stats.get("avgResponseTimeMs"))));
                item.put("recordedAt",     av(Instant.now().toString()));

                dynamoDb.putItem(PutItemRequest.builder()
                        .tableName(TABLE_NAME)
                        .item(item)
                        .build());
            }
            System.out.println("[DynamoDB Analytics] Stored hourly stats for " +
                    hourKey);
        } catch (Exception e) {
            System.err.println("[DynamoDB Analytics] Failed to store hourly stats: "
                    + e.getMessage());
        }
    }

    // ----------------------------
    // Query analytics for a specific service
    // ----------------------------
    public List<Map<String, Object>> getAnalytics(String serviceName, int limit) {
        try {
            ensureTableExists();

            // Scan for records matching the service name
            var response = dynamoDb.scan(ScanRequest.builder()
                    .tableName(TABLE_NAME)
                    .filterExpression("serviceName = :svc")
                    .expressionAttributeValues(Map.of(
                            ":svc", av(serviceName)))
                    .limit(limit * 2)
                    .build());

            List<Map<String, Object>> results = new ArrayList<>();
            for (var item : response.items()) {
                Map<String, Object> record = new LinkedHashMap<>();
                item.forEach((k, v) -> record.put(k, v.s() != null ? v.s() : v.n()));
                results.add(record);
            }

            // Sort by recordedAt descending, limit results
            results.sort((a, b) -> {
                String ta = (String) a.getOrDefault("recordedAt",
                        a.getOrDefault("lastChecked", ""));
                String tb = (String) b.getOrDefault("recordedAt",
                        b.getOrDefault("lastChecked", ""));
                return tb.compareTo(ta);
            });

            return results.subList(0, Math.min(limit, results.size()));

        } catch (Exception e) {
            return List.of(Map.of("error",
                    "Could not retrieve analytics: " + e.getMessage()));
        }
    }

    // ----------------------------
    // Get analytics summary for all services
    // ----------------------------
    public Map<String, Object> getAnalyticsSummary() {
        try {
            ensureTableExists();

            var response = dynamoDb.scan(ScanRequest.builder()
                    .tableName(TABLE_NAME)
                    .filterExpression("#t = :type")
                    .expressionAttributeNames(Map.of("#t", "type"))
                    .expressionAttributeValues(Map.of(":type", av("HOURLY_AGGREGATE")))
                    .build());

            Map<String, List<Double>> uptimeByService = new LinkedHashMap<>();
            for (var item : response.items()) {
                String svc = item.get("serviceName") != null
                        ? item.get("serviceName").s() : "unknown";
                AttributeValue ut = item.get("uptimePercent");
                if (ut != null && ut.n() != null) {
                    uptimeByService.computeIfAbsent(svc, k -> new ArrayList<>())
                            .add(Double.parseDouble(ut.n()));
                }
            }

            Map<String, Object> summary = new LinkedHashMap<>();
            uptimeByService.forEach((svc, uptimes) -> {
                double avg = uptimes.stream().mapToDouble(d -> d).average().orElse(0);
                summary.put(svc, Map.of(
                        "avgUptimePercent", Math.round(avg * 100.0) / 100.0,
                        "dataPoints", uptimes.size()
                ));
            });

            return summary;
        } catch (Exception e) {
            return Map.of("error", "Could not retrieve summary: " + e.getMessage());
        }
    }

    // ----------------------------
    // Helpers
    // ----------------------------
    private AttributeValue av(String s) {
        return AttributeValue.builder().s(s).build();
    }

    private AttributeValue avn(String n) {
        return AttributeValue.builder().n(n != null ? n : "0").build();
    }

    private void ensureTableExists() {
        try {
            dynamoDb.createTable(CreateTableRequest.builder()
                    .tableName(TABLE_NAME)
                    .keySchema(KeySchemaElement.builder()
                            .attributeName("pk")
                            .keyType(KeyType.HASH)
                            .build())
                    .attributeDefinitions(AttributeDefinition.builder()
                            .attributeName("pk")
                            .attributeType(ScalarAttributeType.S)
                            .build())
                    .billingMode(BillingMode.PAY_PER_REQUEST)
                    .build());
            dynamoDb.waiter().waitUntilTableExists(r -> r.tableName(TABLE_NAME));
        } catch (ResourceInUseException e) {
            // Already exists
        } catch (Exception e) {
            // Ignore
        }
    }
}
