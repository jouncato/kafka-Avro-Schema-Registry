package com.litethinking.kafka.broker.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.litethinking.avro.data.PersonPostgresql;

@Service
public class PersonProducer {

	@Autowired
	private KafkaTemplate<String, PersonPostgresql> personKafkaTemplate;
	
	public void publish(PersonPostgresql data) {
		personKafkaTemplate.send("topic-person-address-fake", data);
	}
	
}
