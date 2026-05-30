package uk.ac.ed.inf.cw3.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class SystemEnvironment {

    @Value("${ACP_POSTGRES:jdbc:postgresql://localhost:5432/acp}")
    private String postgresUrl;

    @Value("${ACP_S3:http://localhost:4566}")
    private String s3Endpoint;

    @Value("${ACP_DYNAMODB:http://localhost:4566}")
    private String dynamoEndpoint;

    @Value("${REDIS_HOST:localhost}")
    private String redisHost;

    @Value("${REDIS_PORT:6379}")
    private int redisPort;

    @Value("${RABBITMQ_HOST:localhost}")
    private String rabbitMqHost;

    @Value("${RABBITMQ_PORT:5672}")
    private int rabbitMqPort;

    @Value("${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}")
    private String kafkaBootstrapServers;

    @Value("${health.check.interval:30000}")
    private long healthCheckInterval;

    @Value("${health.check.alert-queue:health-alerts}")
    private String alertQueue;

    @Value("${health.check.kafka-topic:health-events}")
    private String kafkaTopic;

    @Value("${health.check.history-table:health_history}")
    private String historyTable;

    public String getPostgresUrl()           { return postgresUrl; }
    public String getS3Endpoint()            { return s3Endpoint; }
    public String getDynamoEndpoint()        { return dynamoEndpoint; }
    public String getRedisHost()             { return redisHost; }
    public int    getRedisPort()             { return redisPort; }
    public String getRabbitMqHost()          { return rabbitMqHost; }
    public int    getRabbitMqPort()          { return rabbitMqPort; }
    public String getKafkaBootstrapServers() { return kafkaBootstrapServers; }
    public long   getHealthCheckInterval()   { return healthCheckInterval; }
    public String getAlertQueue()            { return alertQueue; }
    public String getKafkaTopic()            { return kafkaTopic; }
    public String getHistoryTable()          { return historyTable; }
}