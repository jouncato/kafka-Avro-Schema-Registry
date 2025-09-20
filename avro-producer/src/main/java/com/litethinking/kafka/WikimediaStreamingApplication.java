package com.litethinking.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.litethinking.kafka.broker.producer.WikimediaProducer;

//@SpringBootApplication
public class WikimediaStreamingApplication implements CommandLineRunner {

    @Autowired
    private WikimediaProducer wikimediaProducer;

    public static void main(String[] args) {
        SpringApplication.run(WikimediaStreamingApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        wikimediaProducer.startStreaming();

        // Keep the application running
        Thread.currentThread().join();
    }
}