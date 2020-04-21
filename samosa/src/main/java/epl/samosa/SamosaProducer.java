package epl.samosa;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.*;
import java.util.concurrent.Future;

public class SamosaProducer<K, V> {
    static GeoHashIndex<String> geoHashIndex;
    StaticTopics staticTopics;
    final Producer<K, V> producer;

    public List<Future<RecordMetadata>> send(SamosaProducerRecord<K, V> record) {
        SortedMap<String, String> neighborMap =
                geoHashIndex.getNearestNeighbors(
                        record.coordinate.getLeft(), record.coordinate.getRight());
        ArrayList<Future<RecordMetadata>> futureArrayList = new ArrayList<>();
        neighborMap.forEach((hash, topic) -> {
            ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, record.key, record.value);
            futureArrayList.add(producer.send(producerRecord));
        });
        return futureArrayList;
    }

    public SamosaProducer (Properties properties) {

        staticTopics = new StaticTopics();

        Properties geoHashIndexProps = new Properties();
        geoHashIndexProps.put("maxCharPrecision", 8);
        geoHashIndex = new GeoHashIndex<>(geoHashIndexProps);
        staticTopics.coordinateMap.forEach((topic, coordinate) ->
            geoHashIndex.addIndex(coordinate.getKey(), coordinate.getRight(), topic)
        );

        producer = new KafkaProducer<>(properties);
    }

    public void close() {
        producer.flush();
        producer.close();
    }

}
