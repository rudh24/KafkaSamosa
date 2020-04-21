package epl.samosa;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.*;

import org.apache.log4j.Logger;


public class SamosaConsumer<K, V> {
    final static Logger logger = Logger.getLogger(SamosaConsumer.class);

    static GeoHashIndex<String> geoHashIndex;
    StaticTopics staticTopics;
    List<String> currentTopics;

    // Create the consumer using props.
    final Consumer<K, V> consumer;

    public SamosaConsumer(Properties props) {

        Properties geoHashIndexProps = new Properties();
        staticTopics = new StaticTopics();
        geoHashIndexProps.put("maxCharPrecision", 8);

        geoHashIndex = new GeoHashIndex<>(geoHashIndexProps);
        staticTopics.coordinateMap.forEach((topic, coordinate) ->
                geoHashIndex.addIndex(coordinate.getKey(), coordinate.getRight(), topic)
        );

        // Create the consumer using props.
        consumer = new KafkaConsumer<>(props);
        currentTopics = new ArrayList<>();
    }

    private List<String> getNeighboringTopics(Pair<Double, Double> coordinate) {
        List<String> topics = new ArrayList<>();
        SortedMap<String, String> neighborMap =
                geoHashIndex.getNearestNeighbors(coordinate.getLeft(), coordinate.getRight());

        neighborMap.forEach((hash, topic) -> topics.add(topic));
        return topics;
    }

    public void subscribe(Pair<Double, Double> coordinate){
        currentTopics.addAll(getNeighboringTopics(coordinate));
        consumer.subscribe(currentTopics);
    }

    public void unsubscribe() {
        consumer.unsubscribe();
        currentTopics.clear();
    }

    public ConsumerRecords<K,V> pollLatest(Pair<Double, Double> coordinate, Long timeout) {
        List<String> topics = getNeighboringTopics(coordinate);
        if(currentTopics.equals(topics)) {
            return consumer.poll(timeout);
        }

        //consumer has moved.
        unsubscribe();
        currentTopics = topics;
        consumer.subscribe(currentTopics);
        return consumer.poll(timeout);
    }

    public void close() {
        consumer.close();
    }

    public void commitAsync() {
       consumer.commitAsync();
    }
}
