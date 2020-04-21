package epl.samosa;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SamosaProducerRecord<K, V> {
    public Pair<Double, Double> coordinate;
    public K key;
    public V value;

    public K key() {
        return key;
    }

    public V value() {
        return value;
    }


    public SamosaProducerRecord(Pair<Double, Double> coordinate, K key, V value) {
        this.coordinate = coordinate;
        this.key = key;
        this.value = value;
    }
}
