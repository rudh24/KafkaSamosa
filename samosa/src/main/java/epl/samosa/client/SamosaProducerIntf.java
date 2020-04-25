package epl.samosa.client;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;

public interface SamosaProducerIntf<K, V> extends SubscriptionChangedCallback<String> {

    Future<RecordMetadata> send(ProducerRecord<K, V> record);
    void close();
    void updateMetrics();
    ProducerMetrics getProducerMetrics();
}
