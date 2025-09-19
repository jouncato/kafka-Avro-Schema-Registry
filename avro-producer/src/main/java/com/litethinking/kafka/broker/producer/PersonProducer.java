package com.litethinking.kafka.broker.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.litethinking.avro.data.PersonPostgresql;

@Service
public class PersonProducer {

	@Autowired
	private KafkaTemplate<String, PersonPostgresql> kafkaTemplate;
	
	public void publish(PersonPostgresql data) {
		kafkaTemplate.send("topic-person-address-postgresql-fake", data);
	}
	
}
