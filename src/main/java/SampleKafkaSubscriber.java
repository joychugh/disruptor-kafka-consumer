import kafka.message.MessageAndMetadata;
import org.apache.log4j.Logger;

import java.util.concurrent.Executor;

/**
 * @author jchugh
 */
public class SampleKafkaSubscriber extends AsyncSubscriber<MessageAndMetadata<byte[], byte[]>> {

    final static Logger logger = Logger.getLogger(SampleKafkaSubscriber.class);

    @Override
    protected boolean whenNext(MessageAndMetadata<byte[], byte[]> element) {
        //System.out.println(new String(element.key()));
        System.out.println(new String(element.message()));
        return true;
    }

    public SampleKafkaSubscriber(Executor executor) {
        super(executor);
    }

    @Override
    protected void whenComplete() {
        super.whenComplete();
    }

    @Override
    protected void whenError(Throwable error) {
        super.whenError(error);
    }
}
