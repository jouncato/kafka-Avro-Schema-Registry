package com.litethinking.kafka.broker.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.litethinking.avro.data.EmployeeForward;

//@Service
public class EmployeeForwardProducer {

    @Autowired
    private KafkaTemplate<String, EmployeeForward> kafkaTemplate;

    public void send(EmployeeForward data) {
        kafkaTemplate.send("topic-employee-forward", data);
    }

}
