package uk.ac.ed.inf.cw3.configuration;

import com.rabbitmq.client.ConnectionFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

import javax.sql.DataSource;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class InfrastructureConfiguration {

    private final SystemEnvironment env;

    public InfrastructureConfiguration(SystemEnvironment env) {
        this.env = env;
    }

    // ----------------------------
    // Postgres
    // ----------------------------
    @Bean
    public DataSource dataSource() {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("org.postgresql.Driver");
        ds.setUrl(env.getPostgresUrl());
        ds.setUsername("postgres");
        ds.setPassword("postgres");
        return ds;
    }

    @Bean
    public JdbcTemplate jdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    // ----------------------------
    // S3
    // ----------------------------
    @Bean
    public S3Client s3Client() {
        return S3Client.builder()
                .endpointOverride(URI.create(env.getS3Endpoint()))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test", "test")))
                .forcePathStyle(true)
                .build();
    }

    // ----------------------------
    // DynamoDB
    // ----------------------------
    @Bean
    public DynamoDbClient dynamoDbClient() {
        return DynamoDbClient.builder()
                .endpointOverride(URI.create(env.getDynamoEndpoint()))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test", "test")))
                .build();
    }

    // ----------------------------
    // Redis
    // ----------------------------
    @Bean
    public JedisPool jedisPool() {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(10);
        config.setMaxIdle(5);
        config.setMinIdle(1);
        // Fixed: disable JMX to avoid MBean registration conflict at startup
        config.setJmxEnabled(false);
        return new JedisPool(config, env.getRedisHost(), env.getRedisPort());
    }

    // ----------------------------
    // RabbitMQ
    // ----------------------------
    @Bean(name = "rabbitConnectionFactory")
    public ConnectionFactory rabbitConnectionFactory() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(env.getRabbitMqHost());
        factory.setPort(env.getRabbitMqPort());
        return factory;
    }

    // ----------------------------
    // Kafka Producer
    // ----------------------------
    @Bean(name = "kafkaProducer")
    public KafkaProducer<String, String> kafkaProducer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, env.getKafkaBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(props);
    }

    // ----------------------------
    // Kafka Consumer Props
    // ----------------------------
    @Bean(name = "kafkaConsumerProps")
    public Map<String, Object> kafkaConsumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, env.getKafkaBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "health-monitor-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return props;
    }

    // ----------------------------
    // Shared ObjectMapper with JSR310 support for java.time.Instant serialization
    // ----------------------------
    @Bean
    public com.fasterxml.jackson.databind.ObjectMapper objectMapper() {
        return new com.fasterxml.jackson.databind.ObjectMapper()
                .registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule())
                .disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    }
}
