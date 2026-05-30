package uk.ac.ed.inf.cw3.service;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class StatsService {
    private final JdbcTemplate jdbcTemplate;

    public StatsService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    // ----------------------------
    // Get stats for all services
    // ----------------------------
    public Map<String, Object> getAllStats() {
        List<String> services = List.of("S3", "DynamoDB", "Postgres",
                "Redis", "RabbitMQ", "Kafka");

        Map<String, Object> result = new LinkedHashMap<>();
        for (String service : services) {
            result.put(service, getServiceStats(service));
        }
        return result;
    }

    // ----------------------------
    // Get stats for a specific service
    // ----------------------------
    public Map<String, Object> getServiceStats(String serviceName) {
        Map<String, Object> stats = new LinkedHashMap<>();

        try {
            // Total checks
            Integer total = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM health_history WHERE service_name = ?",
                    Integer.class, serviceName);

            // UP checks
            Integer upCount = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM health_history WHERE service_name = ? AND status = 'UP'",
                    Integer.class, serviceName);

            // DOWN checks
            Integer downCount = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM health_history WHERE service_name = ? AND status = 'DOWN'",
                    Integer.class, serviceName);

            // Average response time
            Double avgResponseTime = jdbcTemplate.queryForObject(
                    "SELECT AVG(response_time_ms) FROM health_history WHERE service_name = ?",
                    Double.class, serviceName);

            // Last checked
            String lastChecked = null;
            try {
                lastChecked = jdbcTemplate.queryForObject(
                        "SELECT checked_at FROM health_history WHERE service_name = ? " +
                                "ORDER BY checked_at DESC LIMIT 1",
                        String.class, serviceName);
            } catch (Exception e) {
                lastChecked = "Never";
            }

            // Uptime percentage
            double uptimePercent = (total != null && total > 0 && upCount != null)
                    ? Math.round((upCount * 100.0 / total) * 100.0) / 100.0
                    : 0.0;

            stats.put("totalChecks",       total != null ? total : 0);
            stats.put("upCount",           upCount != null ? upCount : 0);
            stats.put("downCount",         downCount != null ? downCount : 0);
            stats.put("uptimePercent",     uptimePercent);
            stats.put("avgResponseTimeMs", avgResponseTime != null
                    ? Math.round(avgResponseTime * 100.0) / 100.0 : 0.0);
            stats.put("lastChecked",       lastChecked);

        } catch (Exception e) {
            stats.put("error", "Could not retrieve stats: " + e.getMessage());
        }

        return stats;
    }
}
