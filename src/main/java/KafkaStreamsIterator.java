import com.google.common.collect.Lists;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

/**
 * @author jchugh
 */
public class KafkaStreamsIterator<K, V> implements Iterator<MessageAndMetadata<K, V>> {

    private final List<KafkaStream<K, V>> kafkaStreams;
    private final List<Iterator<MessageAndMetadata<K, V>>> kafkaIterators;
    private final Integer streamCount;
    private Integer currentStream = 0;

    public KafkaStreamsIterator(List<KafkaStream<K, V>> kafkaStreams) {
        this.kafkaStreams = kafkaStreams;
        streamCount = kafkaStreams.size();
        kafkaIterators = Lists.newArrayList();
        kafkaStreams.stream().map(KafkaStream::iterator).forEach(kafkaIterators::add);
    }

    @Override
    public boolean hasNext() {
        for (int i = 0; i < streamCount; ++i) {
            if (kafkaIterators.get(currentStream).hasNext()) {
                return true;
            } inc();
        }
        return false;
    }

    private Integer inc() {
        if (currentStream < streamCount) {
            if (currentStream == streamCount - 1) {
                currentStream = 0;
                return streamCount - 1;
            }
            return currentStream++;
        } else {
            currentStream = 0;
            return currentStream++;
        }
    }

    @Override
    public MessageAndMetadata<K, V> next() {
        if (hasNext()) {
            return kafkaIterators.get(inc()).next();
        }
        throw new IndexOutOfBoundsException("No more data");
    }

    @Override
    public void remove() {
        throw new NotImplementedException();
    }

    @Override
    public void forEachRemaining(Consumer<? super MessageAndMetadata<K, V>> action) {
        kafkaIterators.stream().forEach(x -> x.forEachRemaining(action));
    }
}
