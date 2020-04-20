package epl.samosa;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SamosaProducerRecord<K, V> {
    Pair<Double, Double> coordinate;
    K key;
    V value;

    public SamosaProducerRecord(Pair<Double, Double> coordinate, K key, V value) {
        this.coordinate = coordinate;
        this.key = key;
        this.value = value;
    }
}
