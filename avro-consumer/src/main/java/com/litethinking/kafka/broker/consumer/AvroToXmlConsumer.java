package com.litethinking.kafka.broker.consumer;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.litethinking.avro.data.Avro01;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class AvroToXmlConsumer {

    // MixIn para ignorar el campo "schema"
    public abstract static class Avro01MixIn {
        @JsonIgnore
        abstract org.apache.avro.Schema getSchema();
    }

    private final ObjectMapper objectMapper;
    private final XmlMapper xmlMapper = new XmlMapper();

    public AvroToXmlConsumer() {
        objectMapper = new ObjectMapper();
        // Aplica el MixIn
        objectMapper.addMixIn(Avro01.class, Avro01MixIn.class);
    }

    @KafkaListener(topics = "topic-schema-avro-01", groupId = "avro-xml-group")
    public void consumeAvroMessage(ConsumerRecord<String, Avro01> record) {
        try {
            Avro01 avroMessage = record.value(); // Kafka ya lo deserializó a Avro01

            // Convertir a JSON eliminando `schema`
            String jsonString = objectMapper.writeValueAsString(avroMessage);
            Map<String, Object> jsonMap = objectMapper.readValue(jsonString, Map.class);

            // Convertir JSON a XML
            String xmlString = xmlMapper.writeValueAsString(jsonMap);

            System.out.println(" XML Output: \n" + xmlString);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}