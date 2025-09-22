package com.litethinking.kafka.broker.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.litethinking.avro.data.Avro01;

//@Service
public class Avro01Consumer {

	private static final Logger LOG = LoggerFactory.getLogger(Avro01Consumer.class);

	@KafkaListener(topics = "topic-schema-avro-01")
	public void listen(ConsumerRecord<String, Avro01> record) {
		LOG.info("{} : {}", record.key(), record.value());
	}

}
