package com.litethinking.kafka.broker.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import com.litethinking.avro.data.PersonPostgresql;
import com.litethinking.avro.data.PersonAddress;

//@Service
public class PersonConsumer {

	private static final Logger LOG = LoggerFactory.getLogger(PersonConsumer.class);

	@KafkaListener(topics = "topic-person-address-fake")
	public void listen(ConsumerRecord<String, Object> record,
			@Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
			@Header(KafkaHeaders.OFFSET) long offset) {
		try {
			if (record.value() instanceof PersonPostgresql) {
				PersonPostgresql person = (PersonPostgresql) record.value();
				LOG.info("PersonPostgresql - Key: {}, Value: {}, Topic: {}, Offset: {}",
					record.key(), person, topic, offset);
			} else if (record.value() instanceof PersonAddress) {
				PersonAddress personAddress = (PersonAddress) record.value();
				LOG.info("PersonAddress - Key: {}, Value: {}, Topic: {}, Offset: {}",
					record.key(), personAddress, topic, offset);
			} else {
				LOG.warn("Unknown message type: {}, Topic: {}, Offset: {}",
					record.value().getClass().getSimpleName(), topic, offset);
			}
		} catch (Exception e) {
			LOG.error("Error processing message at topic: {}, offset: {}, error: {}",
				topic, offset, e.getMessage(), e);
		}
	}

}
