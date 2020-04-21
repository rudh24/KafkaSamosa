import java.util.Collections;
import java.util.Properties;

import epl.samosa.SamosaConsumer;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.*;

public class SimpleConsumer {

    private static SamosaConsumer<Long, String> createConsumer(final String bootstrapServers) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "KafkaExampleConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        return new SamosaConsumer<>(props);
    }

    public static void main(String[] args) {
        String bootstrapServers =
                "localhost:9092,localhost:9093,localhost:9094";
        SamosaConsumer<Long, String> samosaConsumer = createConsumer(bootstrapServers);

        final int giveUp = 100;   int noRecordsCount = 0;

        MutablePair<Double, Double> coordinate = new MutablePair<>(33.781538, -84.401653);
        //Double increment = 0.000200;
        samosaConsumer.subscribe(coordinate);

        while (true) {
            final ConsumerRecords<Long, String> consumerRecords =
                    samosaConsumer.pollLatest(coordinate, (long) 1000);

            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }

            consumerRecords.forEach(record ->
                System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset())
            );

            samosaConsumer.commitAsync();
        }
        samosaConsumer.close();

    }
}
