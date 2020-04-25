package epl.samosa.client;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.LongBinaryOperator;

import org.apache.log4j.Logger;


public class SamosaConsumer<K,V> implements SamosaConsumerIntf<K,V>{
    final static Logger logger = Logger.getLogger(SamosaConsumer.class);

    private final List<String> currentTopics;

    // Create the consumer using props.
    final Consumer<K, V> consumer;

    //Metrics
    private final ConsumerMetrics consumerMetrics = new ConsumerMetrics();

    public SamosaConsumer(Properties props) {
        // Create the consumer using props.
        consumer = new KafkaConsumer<>(props);
        currentTopics = new ArrayList<>();

    }

    public void unsubscribe() {
        consumer.unsubscribe();
        currentTopics.clear();
    }


    public void close() {
        consumer.close();
    }

    public void commitAsync() {
       consumer.commitAsync();
    }

    public void subscribe(List<String> topics) {
       consumer.subscribe(topics);
       currentTopics.addAll(topics);
    }

    public ConsumerRecords<K,V> dynamicPoll(Long timeout) {
        Set<String> subscribedTo = consumer.subscription();
        synchronized (currentTopics) {
            if(!subscribedTo.equals(new HashSet<>(currentTopics))) {
                consumer.unsubscribe();
                consumer.subscribe(currentTopics);
            }
        }
        return consumer.poll(timeout);
    }

    public void onSubscriptionChange(List<String> oldVal, List<String> newVal) {
        //Assumption single topic change. Simple in Kafka. Actual sub/unsub in poll, because maintain threadsafety.
        synchronized (currentTopics) {
            currentTopics.clear();
            currentTopics.addAll(newVal);
        }
    }

    @Override
    public void updateMetrics(ConsumerRecord<K, V> record) {
        consumerMetrics.numMessagesConsumed.getAndIncrement();
        long latency = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis() - record.timestamp()) / 1000;
        consumerMetrics.aggregateEndToEndLatency.getAndAccumulate(latency, Long::sum);
        consumerMetrics.maxEndToEndLatency.getAndAccumulate(latency, Math::max);
    }

    @Override
    public ConsumerMetrics getConsumerMetrics() {
        return consumerMetrics;
    }

}
