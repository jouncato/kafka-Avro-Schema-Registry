package com.litethinking.kafka.broker.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.litethinking.avro.data.PersonPostgresql;

//@Service
public class PersonConsumer {

	private static final Logger LOG = LoggerFactory.getLogger(PersonConsumer.class);
	
	@KafkaListener(topics = "topic-person-address-postgresql")
	public void listen(ConsumerRecord<String, PersonPostgresql> record) {
		LOG.info("{} : {}", record.key(), record.value());
	}
	
}
