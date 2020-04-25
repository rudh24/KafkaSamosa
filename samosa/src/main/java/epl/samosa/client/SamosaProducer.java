package epl.samosa.client;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SamosaProducer<K, V> implements SamosaProducerIntf<K, V>{
    final Producer<K, V> producer;
    private final AtomicReference<String> currentTopic = new AtomicReference<>();
    final private StopWatch sw = new StopWatch();

    private static final Logger log = LoggerFactory.getLogger(SamosaProducer.class);

    //Metrics
    private final ProducerMetrics producerMetrics = new ProducerMetrics();

    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        log.info("SEND TO: {}", currentTopic.get());
        Future<RecordMetadata> futureRecord;
        ProducerRecord<K, V> producerRecord = new ProducerRecord<>(currentTopic.get(), record.value());
        sw.reset();
        sw.start();
        futureRecord = producer.send(producerRecord);
        return futureRecord;
    }

    public SamosaProducer (Properties properties) {
        producer = new KafkaProducer<>(properties);
    }

    public void start(String topic) {
        currentTopic.set(topic);
    }

    public void close() {
        producer.flush();
        producer.close();
    }

    @Override
    public void onSubscriptionChange(String oldVal, String newVal) {
        currentTopic.set(newVal);
    }

    @Override
    public void updateMetrics() {
        sw.stop(); //stopped when future returns in ProducerTask and updateMetrics is the callback used to reflect that.
        producerMetrics.maxPublishLatency.getAndAccumulate(sw.getTime(), Math::max);
        producerMetrics.numMessagesPublished.getAndIncrement();
        producerMetrics.aggregatePublishLatency.getAndAccumulate(sw.getTime(), Long::sum);
    }

    @Override
    public ProducerMetrics getProducerMetrics() {
        return producerMetrics;
    }
}
