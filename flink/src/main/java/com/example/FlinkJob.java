package com.example;

import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.java.*;
import com.couchbase.client.java.json.JsonObject;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkJob {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkJob.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Obtener credenciales de variables de entorno
        String couchbaseHost = getEnv("COUCHBASE_HOST", "couchbase-svc.kafka-dev.svc.cluster.local");
        String couchbaseUsername = getEnv("COUCHBASE_USERNAME", "admin");
        String couchbasePassword = getEnv("COUCHBASE_PASSWORD", "SecurePassword123!");
        String kafkaBrokers = getEnv("KAFKA_BOOTSTRAP_SERVERS", 
            "my-kafka-cluster-kafka-brokers.kafka-dev.svc.cluster.local:9092");

        LOG.info("Starting Flink Job...");
        LOG.info("Kafka Brokers: {}", kafkaBrokers);
        LOG.info("Couchbase Host: {}", couchbaseHost);

        // 1. Configurar Kafka Source
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(kafkaBrokers)
            .setTopics("events")
            .setGroupId("flink-consumer-group")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        // 2. Procesamiento
        DataStream<String> processedStream = env
            .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
            .map(event -> {
                try {
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode jsonNode = mapper.readTree(event);

                    // Validación: debe tener eventId
                    if (!jsonNode.has("eventId") || 
                        jsonNode.get("eventId").asText().isEmpty()) {
                        LOG.warn("Event without eventId, discarding: {}", event);
                        return null;
                    }

                    // Enriquecimiento
                    JsonObject enrichedEvent = JsonObject.create()
                        .put("eventId", jsonNode.get("eventId").asText())
                        .put("type", jsonNode.path("type").asText("UNKNOWN"))
                        .put("customerId", jsonNode.path("customerId").asText("UNKNOWN"))
                        .put("amount", jsonNode.path("amount").asDouble(0.0))
                        .put("originalTimestamp", jsonNode.path("timestamp").asText(""))
                        .put("processed", true)
                        .put("processedBy", "flink")
                        .put("processedAt", java.time.Instant.now().toString());

                    LOG.debug("Processed event: {}", enrichedEvent);
                    return enrichedEvent.toString();
                } catch (Exception e) {
                    LOG.error("Error processing event: {}", event, e);
                    return null;
                }
            })
            .filter(event -> event != null)
            .name("Validation & Transformation");

        // 3. Couchbase Sink con retry y idempotencia
        processedStream.addSink(new RichSinkFunction<String>() {
            private transient Cluster cluster;
            private transient Collection collection;

            @Override
            public void open(Configuration parameters) throws Exception {
                LOG.info("Connecting to Couchbase at {}", couchbaseHost);
                
                cluster = Cluster.connect(
                    couchbaseHost,
                    ClusterOptions.clusterOptions(couchbaseUsername, couchbasePassword)
                );
                
                Bucket bucket = cluster.bucket("events");
                bucket.waitUntilReady(java.time.Duration.ofSeconds(30));
                collection = bucket.scope("application").collection("processed-events");
                
                LOG.info("Connected to Couchbase successfully");
            }

            @Override
            public void invoke(String value, Context context) throws Exception {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode jsonNode = mapper.readTree(value);
                String docId = jsonNode.get("eventId").asText();
                JsonObject document = JsonObject.fromJson(value);

                int maxRetries = 3;
                int retryDelay = 1000;

                for (int attempt = 1; attempt <= maxRetries; attempt++) {
                    try {
                        collection.insert(docId, document);
                        LOG.debug("Document inserted: {}", docId);
                        return;
                    } catch (DocumentExistsException e) {
                        // Idempotencia: actualizar si ya existe
                        collection.replace(docId, document);
                        LOG.debug("Document replaced: {}", docId);
                        return;
                    } catch (TimeoutException e) {
                        if (attempt == maxRetries) {
                            LOG.error("Failed after {} attempts: {}", maxRetries, docId, e);
                            throw e;
                        }
                        LOG.warn("Timeout, attempt {}/{}", attempt, maxRetries);
                        Thread.sleep(retryDelay * attempt);
                    } catch (Exception e) {
                        LOG.error("Unexpected error: {}", docId, e);
                        throw e;
                    }
                }
            }

            @Override
            public void close() throws Exception {
                if (cluster != null) {
                    LOG.info("Closing Couchbase connection");
                    cluster.close();
                }
            }
        }).name("Couchbase Sink");

        // 4. Ejecutar
        env.execute("Kafka to Couchbase ETL");
    }

    private static String getEnv(String name, String defaultValue) {
        String value = System.getenv(name);
        return value != null ? value : defaultValue;
    }
}