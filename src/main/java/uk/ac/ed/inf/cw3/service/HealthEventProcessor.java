package uk.ac.ed.inf.cw3.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import uk.ac.ed.inf.cw3.configuration.SystemEnvironment;
import uk.ac.ed.inf.cw3.model.HealthEvent;
import uk.ac.ed.inf.cw3.model.ServiceStatus;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

@Service
public class HealthEventProcessor {

    private final JdbcTemplate jdbcTemplate;
    private final ConnectionFactory rabbitConnectionFactory;
    private final SystemEnvironment env;
    private final com.fasterxml.jackson.databind.ObjectMapper mapper;

    public HealthEventProcessor(
            JdbcTemplate jdbcTemplate,
            @Qualifier("rabbitConnectionFactory") ConnectionFactory rabbitConnectionFactory,
            SystemEnvironment env,
            com.fasterxml.jackson.databind.ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.rabbitConnectionFactory = rabbitConnectionFactory;
        this.env = env;
        this.mapper = objectMapper;
    }

    // ----------------------------
    // Process a batch of health events
    // Stores in Postgres and sends alerts for DOWN/DEGRADED services
    // ----------------------------
    public void processEvents(List<HealthEvent> events) {
        ensureHistoryTableExists();

        for (HealthEvent event : events) {
            storeInPostgres(event);

            if (event.getStatus() == ServiceStatus.DOWN ||
                    event.getStatus() == ServiceStatus.DEGRADED) {
                sendAlert(event);
            }
        }
    }

    // ----------------------------
    // Store health event in Postgres history table
    // ----------------------------
    private void storeInPostgres(HealthEvent event) {
        try {
            String sql = """
                    INSERT INTO health_history
                    (service_name, status, message, response_time_ms,
                     checked_at, recovery_attempted, recovery_successful)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """;

            jdbcTemplate.update(sql,
                    event.getServiceName(),
                    event.getStatus().name(),
                    event.getMessage(),
                    event.getResponseTimeMs(),
                    java.sql.Timestamp.from(event.getTimestamp()),
                    event.isRecoveryAttempted(),
                    event.isRecoverySuccessful()
            );
        } catch (Exception e) {
            System.err.println("Failed to store health event in Postgres: " + e.getMessage());
        }
    }

    // ----------------------------
    // Send alert to RabbitMQ for DOWN/DEGRADED services
    // ----------------------------
    private void sendAlert(HealthEvent event) {
        try (Connection connection = rabbitConnectionFactory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.queueDeclare(env.getAlertQueue(), true, false, false, null);

            String json = mapper.writeValueAsString(event);
            channel.basicPublish("", env.getAlertQueue(), null,
                    json.getBytes(StandardCharsets.UTF_8));

        } catch (Exception e) {
            System.err.println("Failed to send alert to RabbitMQ: " + e.getMessage());
        }
    }

    // ----------------------------
    // Create health_history table if it doesn't exist
    // ----------------------------
    public void ensureHistoryTableExists() {
        try {
            String sql = """
                    CREATE TABLE IF NOT EXISTS health_history (
                        id SERIAL PRIMARY KEY,
                        service_name VARCHAR(50) NOT NULL,
                        status VARCHAR(20) NOT NULL,
                        message TEXT,
                        response_time_ms BIGINT,
                        checked_at TIMESTAMP NOT NULL,
                        recovery_attempted BOOLEAN DEFAULT FALSE,
                        recovery_successful BOOLEAN DEFAULT FALSE
                    )
                    """;
            jdbcTemplate.execute(sql);
        } catch (Exception e) {
            System.err.println("Failed to create health_history table: " + e.getMessage());
        }
    }

    // ----------------------------
    // Read health history from Postgres
    // ----------------------------
    public List<Map<String, Object>> getHistory(String serviceName, int limit) {
        try {
            if (serviceName != null && !serviceName.isBlank()) {
                return jdbcTemplate.queryForList(
                        "SELECT * FROM health_history WHERE service_name = ? " +
                                "ORDER BY checked_at DESC LIMIT ?",
                        serviceName, limit);
            }
            return jdbcTemplate.queryForList(
                    "SELECT * FROM health_history ORDER BY checked_at DESC LIMIT ?",
                    limit);
        } catch (Exception e) {
            return List.of();
        }
    }
}
