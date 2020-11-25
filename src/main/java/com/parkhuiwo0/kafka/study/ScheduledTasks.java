package com.parkhuiwo0.kafka.study;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.Date;

@Component
public class ScheduledTasks {

    private static final Logger log = LoggerFactory.getLogger(ScheduledTasks.class);

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    public ScheduledTasks(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(String topic, String payload) {
        kafkaTemplate.send(topic, payload);
        log.info("Message: " + payload + " sent to topic: " + topic);
    }


    @Scheduled(fixedRate = 1000)
    public void reportCurrentTime() {
        send("test", "helloworld " + dateFormat.format(new Date()));
    }

    @KafkaListener(topics = "test")
    public void receiveTopic1(ConsumerRecord consumerRecord) {
        log.info("Receiver on topic1: "+consumerRecord.toString());
    }

}

