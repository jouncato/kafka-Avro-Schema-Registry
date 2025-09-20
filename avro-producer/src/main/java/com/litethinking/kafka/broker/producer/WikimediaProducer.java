package com.litethinking.kafka.broker.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.litethinking.avro.wikimedia.WikimediaRecentChange;
import com.litethinking.avro.wikimedia.PartitionOffset;
import com.litethinking.avro.wikimedia.ChangeData;
import com.litethinking.avro.wikimedia.Meta;
import com.litethinking.avro.wikimedia.Length;
import com.litethinking.avro.wikimedia.Revision;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//@Service
public class WikimediaProducer {

    private static final Logger logger = LoggerFactory.getLogger(WikimediaProducer.class);
    private static final String WIKIMEDIA_URL = "https://stream.wikimedia.org/v2/stream/recentchange";
    private static final String KAFKA_TOPIC = "topic-wikimedia-stream-avro";

    @Autowired
    private KafkaTemplate<String, WikimediaRecentChange> kafkaTemplate;

    private final WebClient webClient;
    private final ObjectMapper objectMapper;

    public WikimediaProducer() {
        this.webClient = WebClient.builder()
                .baseUrl(WIKIMEDIA_URL)
                .build();
        this.objectMapper = new ObjectMapper();
    }

    public void startStreaming() {
        logger.info("Starting Wikimedia streaming from: {}", WIKIMEDIA_URL);

        webClient.get()
                .retrieve()
                .bodyToFlux(String.class)
                .filter(line -> line.startsWith("data: "))
                .map(line -> line.substring(6)) // Remove "data: " prefix
                .filter(data -> !data.trim().isEmpty())
                .flatMap(this::parseAndConvert)
                .doOnNext(this::publishToKafka)
                .doOnError(error -> logger.error("Error in streaming: ", error))
                .retry(5)
                .subscribe();
    }

    private Mono<WikimediaRecentChange> parseAndConvert(String jsonData) {
        return Mono.fromCallable(() -> {
            try {
                JsonNode rootNode = objectMapper.readTree(jsonData);
                return convertToAvro(rootNode);
            } catch (JsonProcessingException e) {
                logger.error("Error parsing JSON: {}", jsonData, e);
                throw new RuntimeException(e);
            }
        });
    }

    private WikimediaRecentChange convertToAvro(JsonNode rootNode) {
        WikimediaRecentChange.Builder builder = WikimediaRecentChange.newBuilder();

        // Set event field (from SSE event type, defaulting to "message")
        builder.setEvent("message");

        // Parse and set id field (array of partition offsets)
        List<PartitionOffset> partitionOffsets = new ArrayList<>();
        // For SSE format, id is typically in the header, but we'll create a default one
        PartitionOffset.Builder offsetBuilder = PartitionOffset.newBuilder();
        offsetBuilder.setTopic("mediawiki.recentchange");
        offsetBuilder.setPartition(0);
        offsetBuilder.setOffset(System.currentTimeMillis());
        offsetBuilder.setTimestamp(System.currentTimeMillis());
        partitionOffsets.add(offsetBuilder.build());
        builder.setId(partitionOffsets);

        // Parse and set data field
        ChangeData.Builder dataBuilder = ChangeData.newBuilder();

        // Schema field
        if (rootNode.has("$schema")) {
            dataBuilder.setSchema$(rootNode.get("$schema").asText());
        } else {
            dataBuilder.setSchema$("/mediawiki/recentchange/1.0.0");
        }

        // Meta field
        if (rootNode.has("meta")) {
            JsonNode metaNode = rootNode.get("meta");
            Meta.Builder metaBuilder = Meta.newBuilder();

            metaBuilder.setUri(getStringValue(metaNode, "uri", ""));
            metaBuilder.setRequestId(getStringValue(metaNode, "request_id", ""));
            metaBuilder.setId(getStringValue(metaNode, "id", ""));
            metaBuilder.setDt(getStringValue(metaNode, "dt", ""));
            metaBuilder.setDomain(getStringValue(metaNode, "domain", ""));
            metaBuilder.setStream(getStringValue(metaNode, "stream", ""));
            metaBuilder.setTopic(getStringValue(metaNode, "topic", ""));
            metaBuilder.setPartition(getIntValue(metaNode, "partition", 0));
            metaBuilder.setOffset(getLongValue(metaNode, "offset", 0L));

            dataBuilder.setMeta(metaBuilder.build());
        }

        // Main data fields
        dataBuilder.setId(getLongValue(rootNode, "id", 0L));
        dataBuilder.setType(getStringValue(rootNode, "type", ""));
        dataBuilder.setNamespace(getIntValue(rootNode, "namespace", 0));
        dataBuilder.setTitle(getStringValue(rootNode, "title", ""));
        dataBuilder.setTitleUrl(getStringValue(rootNode, "title_url", ""));
        dataBuilder.setComment(getStringValue(rootNode, "comment", ""));
        dataBuilder.setTimestamp(getLongValue(rootNode, "timestamp", 0L));
        dataBuilder.setUser(getStringValue(rootNode, "user", ""));
        dataBuilder.setBot(getBooleanValue(rootNode, "bot", false));
        dataBuilder.setNotifyUrl(getStringValue(rootNode, "notify_url", ""));
        dataBuilder.setServerUrl(getStringValue(rootNode, "server_url", ""));
        dataBuilder.setServerName(getStringValue(rootNode, "server_name", ""));
        dataBuilder.setServerScriptPath(getStringValue(rootNode, "server_script_path", ""));
        dataBuilder.setWiki(getStringValue(rootNode, "wiki", ""));
        dataBuilder.setParsedcomment(getStringValue(rootNode, "parsedcomment", ""));

        // Optional fields
        if (rootNode.has("minor")) {
            dataBuilder.setMinor(rootNode.get("minor").asBoolean());
        }

        if (rootNode.has("length")) {
            JsonNode lengthNode = rootNode.get("length");
            Length.Builder lengthBuilder = Length.newBuilder();
            lengthBuilder.setOld(getIntValue(lengthNode, "old", 0));
            lengthBuilder.setNew$(getIntValue(lengthNode, "new", 0));
            dataBuilder.setLength(lengthBuilder.build());
        }

        if (rootNode.has("revision")) {
            JsonNode revisionNode = rootNode.get("revision");
            Revision.Builder revisionBuilder = Revision.newBuilder();
            revisionBuilder.setOld(getLongValue(revisionNode, "old", 0L));
            revisionBuilder.setNew$(getLongValue(revisionNode, "new", 0L));
            dataBuilder.setRevision(revisionBuilder.build());
        }

        builder.setData(dataBuilder.build());

        return builder.build();
    }

    private void publishToKafka(WikimediaRecentChange data) {
        try {
            kafkaTemplate.send(KAFKA_TOPIC, data);
            logger.debug("Published message to Kafka topic: {}", KAFKA_TOPIC);
        } catch (Exception e) {
            logger.error("Error publishing to Kafka: ", e);
        }
    }

    // Helper methods for safe JSON parsing
    private String getStringValue(JsonNode node, String fieldName, String defaultValue) {
        return node.has(fieldName) ? node.get(fieldName).asText() : defaultValue;
    }

    private int getIntValue(JsonNode node, String fieldName, int defaultValue) {
        return node.has(fieldName) ? node.get(fieldName).asInt() : defaultValue;
    }

    private long getLongValue(JsonNode node, String fieldName, long defaultValue) {
        return node.has(fieldName) ? node.get(fieldName).asLong() : defaultValue;
    }

    private boolean getBooleanValue(JsonNode node, String fieldName, boolean defaultValue) {
        return node.has(fieldName) ? node.get(fieldName).asBoolean() : defaultValue;
    }

    public void stopStreaming() {
        logger.info("Stopping Wikimedia streaming");
        // Implementation for graceful shutdown if needed
    }
}