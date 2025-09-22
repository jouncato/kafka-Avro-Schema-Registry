package com.litethinking.kafka.broker.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.litethinking.avro.data.PersonAddress;

@Service
public class PersonProducer {

	@Autowired
    private KafkaTemplate<String, PersonAddress> kafkaTemplate;

    public void publish(PersonAddress data) {
        kafkaTemplate.send("topic-person-address-fake", data);
    }
	
}
