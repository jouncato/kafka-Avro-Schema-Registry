package com.litethinking.kafka;

import java.util.concurrent.ThreadLocalRandom;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import com.litethinking.avro.data.Avro01;
import com.litethinking.kafka.broker.producer.Avro01Producer;

@SpringBootApplication
@EnableScheduling 
public class KafkaCoreProducerApplicationAvro01 implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(KafkaCoreProducerApplicationAvro01.class, args);
    }

    @Autowired
    private Avro01Producer producer;

    private volatile boolean keepSending = true;

    @Override
    public void run(String... args) throws Exception {
   
        System.out.println("Producer started. Sending messages...");
    }

    @Scheduled(fixedRate = 2000) 
    public void sendScheduledMessage() {
        if (!keepSending) return;

        var data = Avro01.newBuilder()
            .setFullName("Full name " + ThreadLocalRandom.current().nextInt())
            .setActive(true)
            .setMaritalStatus("SINGLE")
            .build();

        producer.send(data);
        System.out.println("Message sent: " + data.getFullName());
    }

    
    public void stopSending() {
        keepSending = false;
        System.out.println("Stopped sending messages.");
    }
}