import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * @author jchugh
 */
public class KafkaStreamsIterable implements Iterable<MessageAndMetadata<byte[], byte[]>> {

    private final Iterator<MessageAndMetadata<byte[], byte[]>> iterator;

    public KafkaStreamsIterable(List<KafkaStream<byte[], byte[]>> kafkaStreams) {
        this.iterator = new KafkaStreamsIterator<>(kafkaStreams);
    }

    @Override
    public Iterator<MessageAndMetadata<byte[], byte[]>> iterator() {
        return iterator;
    }

    @Override
    public void forEach(Consumer<? super MessageAndMetadata<byte[], byte[]>> action) {
        iterator().forEachRemaining(action);
    }

    @Override
    public Spliterator<MessageAndMetadata<byte[], byte[]>> spliterator() {
        throw new NotImplementedException();
    }
}
