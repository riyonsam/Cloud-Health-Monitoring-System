package uk.ac.ed.inf.cw3.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import uk.ac.ed.inf.cw3.configuration.SystemEnvironment;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Service
public class ReportService {

    private static final String REPORT_BUCKET = "health-reports";
    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss")
                    .withZone(ZoneId.of("UTC"));

    private final S3Client s3Client;
    private final StatsService statsService;
    private final HealthEventProcessor eventProcessor;
    private final HealthMonitorService monitorService;
    private final JedisPool jedisPool;
    private final SystemEnvironment env;
    private final ObjectMapper mapper;

    public ReportService(
            S3Client s3Client,
            StatsService statsService,
            HealthEventProcessor eventProcessor,
            HealthMonitorService monitorService,
            JedisPool jedisPool,
            SystemEnvironment env,
            ObjectMapper objectMapper) {
        this.s3Client = s3Client;
        this.statsService = statsService;
        this.eventProcessor = eventProcessor;
        this.monitorService = monitorService;
        this.jedisPool = jedisPool;
        this.env = env;
        this.mapper = objectMapper;
    }

    // ----------------------------
    // Generate a full health report and save to S3
    // Returns the S3 key of the saved report
    // ----------------------------
    public Map<String, Object> generateAndSaveReport() throws Exception {
        ensureBucketExists();

        Instant now = Instant.now();
        String reportKey = "report_" + FORMATTER.format(now) + ".json";

        // Build report
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("generatedAt",   now.toString());
        report.put("reportKey",     reportKey);

        // Current status of all services
        List<String> services = List.of("S3", "DynamoDB", "Postgres",
                "Redis", "RabbitMQ", "Kafka");
        Map<String, Object> currentStatus = new LinkedHashMap<>();
        for (String service : services) {
            int failures = monitorService.getConsecutiveFailures(service);
            Map<String, Object> svc = new LinkedHashMap<>(
                    (Map<String, Object>) statsService.getServiceStats(service));
            svc.put("consecutiveFailures", failures);
            currentStatus.put(service, svc);
        }
        report.put("serviceStats", currentStatus);

        // Recent history (last 20 entries)
        report.put("recentHistory", eventProcessor.getHistory(null, 20));

        // Overall summary - based on CURRENT live status from Redis
        Map<String, Object> summary = new LinkedHashMap<>();
        long liveUpCount = 0;
        try (Jedis jedis = jedisPool.getResource()) {
            for (String service : services) {
                String cached = jedis.get("health:status:" + service);
                if (cached != null && cached.startsWith("UP")) {
                    liveUpCount++;
                }
            }
        } catch (Exception e) {
            // Fall back to stats-based calculation if Redis unavailable
            liveUpCount = currentStatus.values().stream()
                    .filter(v -> {
                        Map<?, ?> m = (Map<?, ?>) v;
                        Object ut = m.get("uptimePercent");
                        return ut != null && ((Number) ut).doubleValue() > 50.0;
                    }).count();
        }
        summary.put("totalServices",     services.size());
        summary.put("healthyServices",   liveUpCount);
        summary.put("unhealthyServices", services.size() - liveUpCount);
        // HEALTHY = all UP, DEGRADED = more than half UP, CRITICAL = half or less UP
        summary.put("overallHealth",     liveUpCount == services.size() ? "HEALTHY"
                : liveUpCount > services.size() / 2 ? "DEGRADED" : "CRITICAL");
        summary.put("definitionNote",    "healthy = currently UP (live status from Redis)");
        report.put("summary", summary);

        // Save to S3
        String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(report);
        s3Client.putObject(
                PutObjectRequest.builder()
                        .bucket(REPORT_BUCKET)
                        .key(reportKey)
                        .contentType("application/json")
                        .build(),
                RequestBody.fromString(json)
        );

        return Map.of(
                "reportKey",   reportKey,
                "bucket",      REPORT_BUCKET,
                "generatedAt", now.toString(),
                "sizeBytes",   json.length(),
                "summary",     summary
        );
    }

    // ----------------------------
    // List all saved reports in S3
    // ----------------------------
    public List<Map<String, Object>> listReports() {
        try {
            ensureBucketExists();
            var response = s3Client.listObjectsV2(
                    ListObjectsV2Request.builder().bucket(REPORT_BUCKET).build());

            List<Map<String, Object>> reports = new ArrayList<>();
            for (var obj : response.contents()) {
                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("key",          obj.key());
                entry.put("sizeBytes",    obj.size());
                entry.put("lastModified", obj.lastModified().toString());
                reports.add(entry);
            }
            // Most recent first
            reports.sort((a, b) -> b.get("lastModified").toString()
                    .compareTo(a.get("lastModified").toString()));
            return reports;
        } catch (Exception e) {
            return List.of(Map.of("error", "Could not list reports: " + e.getMessage()));
        }
    }

    // ----------------------------
    // Fetch a specific report from S3 by key
    // ----------------------------
    public Map<String, Object> getReport(String key) throws Exception {
        String json = s3Client.getObjectAsBytes(
                GetObjectRequest.builder().bucket(REPORT_BUCKET).key(key).build()
        ).asUtf8String();
        return mapper.readValue(json, Map.class);
    }

    // ----------------------------
    // Ensure the reports bucket exists
    // ----------------------------
    private void ensureBucketExists() {
        try {
            s3Client.createBucket(CreateBucketRequest.builder()
                    .bucket(REPORT_BUCKET).build());
        } catch (BucketAlreadyExistsException | BucketAlreadyOwnedByYouException e) {
            // Already exists
        } catch (Exception e) {
            // Ignore
        }
    }
}