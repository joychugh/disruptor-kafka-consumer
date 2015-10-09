import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.reactivestreams.Publisher;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;

/**
 * @author jchugh
 */
public class KafkaConsumer {

    private final String topic;
    private final Integer numThreads;
    private final Map<String, Integer> topicCountMap = new HashMap<>();
    private final Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap;
    private final ConsumerConnector consumer;
    List<KafkaStream<byte[], byte[]>> kafkaStreams;

    public KafkaConsumer(String topic, Integer numThreads, ConsumerConfig consumerConfig) {
        this.topic = topic;
        this.numThreads = numThreads;
        topicCountMap.put(topic, numThreads);
        consumer = Consumer.createJavaConsumerConnector(consumerConfig);
        consumerMap = consumer.createMessageStreams(topicCountMap);
        kafkaStreams = consumerMap.get(topic);
    }

    public Publisher<MessageAndMetadata<byte[], byte[]>> getRXPublisher() {
        return new AsyncIterablePublisher<>(new KafkaStreamsIterable(kafkaStreams), Executors.newFixedThreadPool(numThreads));
    }

    public static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(props);
    }
}
