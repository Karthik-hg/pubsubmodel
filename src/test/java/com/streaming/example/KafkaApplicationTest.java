package com.streaming.example;

import com.streaming.example.consumer.Consumer;
import com.streaming.example.producer.Producer;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@SpringBootTest
public class KafkaApplicationTest {

    private static String KAFKA_TOPIC = "KafkaTopic";

    @Autowired
    private Producer producer;

    @Autowired
    private Consumer consumer;

    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, KAFKA_TOPIC);

    @Test
    public void testReceive() throws Exception {
        producer.send(KAFKA_TOPIC, "Hello Consumers!");

        consumer.getLatch().await(10000, TimeUnit.MILLISECONDS);
        consumer.receive(producer.getClass().toString());
        assertEquals(0,consumer.getLatch().getCount());
    }
}
