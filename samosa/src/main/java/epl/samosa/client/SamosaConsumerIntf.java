package epl.samosa.client;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.List;

public interface SamosaConsumerIntf<K,V> extends SubscriptionChangedCallback<List<String>>{
    ConsumerRecords<K,V> dynamicPoll(Long timeout);
    void close();
    void commitAsync();
    void subscribe(List<String> topics);
    void unsubscribe();
    void updateMetrics(ConsumerRecord<K,V> record);
    ConsumerMetrics getConsumerMetrics();
}
