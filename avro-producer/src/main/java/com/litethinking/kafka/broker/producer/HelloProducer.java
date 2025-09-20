package com.litethinking.kafka.broker.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.litethinking.avro.data.HolaAvro;

@Service
public class HelloProducer {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    public void send(HolaAvro data) {
        kafkaTemplate.send("topic-hello", data);
    }

}
