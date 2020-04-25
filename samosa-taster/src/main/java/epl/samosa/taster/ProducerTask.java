package epl.samosa.taster;

import epl.samosa.client.SamosaProducer;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ProducerTask implements Runnable {

    private final SamosaProducer<Long, String> producer;
    private final String payload;
    private final int interval;
    private final String initialTopic;

    private final AtomicBoolean isStarted = new AtomicBoolean();

    private static final Logger log = LoggerFactory.getLogger(ProducerTask.class);


    public ProducerTask(SamosaProducer<Long, String> producer, String payload, int interval, String initialTopic) {
        this.producer = producer;
        this.payload = payload;
        this.interval = interval;
        this.initialTopic = initialTopic;
        isStarted.set(false);
    }

    @Override
    public void run(){
        while(isStarted.get()){
            try {
                final ProducerRecord<Long, String> producerRecord = new ProducerRecord<>(initialTopic, payload);
                Future<RecordMetadata> futureRecord = producer.send(producerRecord);
                RecordMetadata record;
                try {
                    record = futureRecord.get();
//                    log.debug(String.format("Metadata record timestamp: [%d]",record.timestamp()));
                    producer.updateMetrics();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
//                log.info("sent message");
                Thread.sleep(interval);
            } catch(InterruptedException e){
                e.printStackTrace();
            }
        }
    }
    void start() {
        isStarted.set(true);
        producer.start(initialTopic);
    }
    void stop(){
        isStarted.set(false);
    }
}