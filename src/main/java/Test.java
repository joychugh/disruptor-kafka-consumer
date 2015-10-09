import java.util.concurrent.Executors;

/**
 * @author jchugh
 */
public class Test {

    public static void main(String[] args) {
        KafkaConsumer kafkaConsumer = new KafkaConsumer("test", 3, KafkaConsumer.createConsumerConfig("localhost:2181", "m3ygrp0"));
        kafkaConsumer.getRXPublisher().subscribe(new SampleKafkaSubscriber(Executors.newFixedThreadPool(3)));
    }
}
