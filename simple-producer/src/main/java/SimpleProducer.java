import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import epl.samosa.SamosaProducer;
import epl.samosa.SamosaProducerRecord;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;


public class SimpleProducer {
    private static SamosaProducer<Long, String> createProducer(final String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        return new SamosaProducer<>(props);
    }
    
    public static void main(String[] args) {
        System.out.println("Hello Pub Sub!");

        //kafka
        String bootstrapServers =
                "localhost:9092,localhost:9093,localhost:9094";

        //specify region bounds
        SamosaProducer<Long, String> producer = createProducer(bootstrapServers);
        int sendMessageCount = 5;
        long time = System.currentTimeMillis();
        MutablePair<Double, Double> coordinate = new MutablePair<>(33.781538, -84.401653);
        //Double increment = 0.000200;
        try {
            for (long index = time; index < time + sendMessageCount; index++) {
                final SamosaProducerRecord<Long, String> record =
                        new SamosaProducerRecord<>(coordinate, index,
                                "Hello Samosa!");

                List<Future<RecordMetadata>> futureList = producer.send(record);
                List<RecordMetadata> metadataList = new ArrayList<>();
                futureList.forEach(future -> {
                    try {
                        metadataList.add(future.get());
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                });

                metadataList.forEach(metadata -> {

                    long elapsedTime = System.currentTimeMillis() - time;
                    System.out.printf("sent record(key=%s value=%s) " +
                                    "meta(partition=%d, offset=%d) time=%d\n",
                            record.key(), record.value(), metadata.partition(),
                            metadata.offset(), elapsedTime);
                });

            }
        }
        catch (Exception exception) {
            exception.printStackTrace();
        }
        finally {
            producer.close();
        }
    }
}
