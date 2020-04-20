package epl.samosa;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.*;

public class SamosaConsumer<K, V> {
    static GeoHashIndex<String> geoHashIndex;
    Map<String, Pair<Double, Double>> staticTopicCoordinateMap;
    // Create the consumer using props.
    final Consumer<K, V> consumer;
    List<String> currentTopics;

    public SamosaConsumer(Properties props) {

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
}
