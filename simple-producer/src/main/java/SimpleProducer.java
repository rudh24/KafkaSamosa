import java.util.Properties;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;


public class SimpleProducer {
    private static Producer<Long, String> createProducer(final String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        return new KafkaProducer<>(props);
    }
    
    public static void main(String[] args) {
        System.out.println("Hello Pub Sub!");

        //kafka
        String bootstrapServers =
                "localhost:9092,localhost:9093,localhost:9094";

        //specify region bounds
        String topic = "my-example-topic"; // <--- this is dynamic for samosa
        Producer<Long, String> producer = createProducer(bootstrapServers);
        int sendMessageCount = 5;
        long time = System.currentTimeMillis();
        try {
            for (long index = time; index < time + sendMessageCount; index++) {
                final ProducerRecord<Long, String> record =
                        new ProducerRecord<>(topic, index,
                                "Hello smoll Consumer! " + index);

                RecordMetadata metadata = producer.send(record).get();

                long elapsedTime = System.currentTimeMillis() - time;
                System.out.printf("sent record(key=%s value=%s) " +
                                "meta(partition=%d, offset=%d) time=%d\n",
                        record.key(), record.value(), metadata.partition(),
                        metadata.offset(), elapsedTime);

            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        finally {
            producer.flush();
            producer.close();
        }
    }
}
