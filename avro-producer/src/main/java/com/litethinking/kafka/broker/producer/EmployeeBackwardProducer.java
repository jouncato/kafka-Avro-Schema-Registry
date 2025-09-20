package com.litethinking.kafka.broker.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.litethinking.avro.data.EmployeeBackward;

//@Service
public class EmployeeBackwardProducer {

    @Autowired
    private KafkaTemplate<String, EmployeeBackward> kafkaTemplate;

    public void send(EmployeeBackward data) {
        kafkaTemplate.send("topic-employee-backward", data);
    }

}
