package com.litethinking.kafka.scheduler;

import java.util.concurrent.ThreadLocalRandom;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.github.javafaker.Faker;
import com.litethinking.avro.data.PersonPostgresql;
import com.litethinking.kafka.broker.producer.PersonProducer;

//@Service
public class PersonAddressScheduler {

	private Faker faker = Faker.instance();

	@Autowired
	private PersonProducer producer;

	//Dummy Data se crea
	private PersonPostgresql fakePerson() {
		var person = new PersonPostgresql();

		person.setPersonId(Integer.toString(ThreadLocalRandom.current().nextInt(1, 1000000)));
		person.setEmail(faker.internet().emailAddress());
		person.setFullName(faker.name().fullName());
		person.setAddressId(Integer.toString(ThreadLocalRandom.current().nextInt(1, 1000000)));
		person.setAddress(faker.address().streetAddress());
		person.setCity(faker.address().city());
		person.setPostalCode(faker.address().zipCode());

		return person;
	}
	
	@Scheduled(fixedRate = 2000)
	public void publishFakePerson() {
		producer.publish(fakePerson());
	}

}
