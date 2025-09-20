package com.litethinking.kafka;

import java.util.concurrent.ThreadLocalRandom;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import com.litethinking.kafka.broker.producer.HelloProducer;
import com.litethinking.avro.data.HolaAvro;

@SpringBootApplication
@EnableScheduling
public class KafkaCoreProducerApplication implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(KafkaCoreProducerApplication.class, args);
    }

    @Autowired
    private HelloProducer helloProducer;

    private volatile boolean keepSending = true;

    @Override
    public void run(String... args) throws Exception {
        System.out.println("Hola Producer started. Sending messages...");
    }

    @Scheduled(fixedRate = 2000)
    public void sendScheduledMessage() {
        if (!keepSending) return;

        HolaAvro data = HolaAvro.newBuilder()
            .setMyStringField("Mensaje aleatorio " + ThreadLocalRandom.current().nextInt())
            .setMyIntField(ThreadLocalRandom.current().nextInt(0, 1000))
            .build();

        helloProducer.send(data);
        System.out.println("Mensaje enviado: " + data.getMyStringField() + " - " + data.getMyIntField());
    }

    public void stopSending() {
        keepSending = false;
        System.out.println("Envío detenido.");
    }
}