package epl.samosa.taster;

import epl.samosa.client.SamosaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.log4j.Logger;
import org.omg.CosNaming.BindingIterator;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerTask implements Runnable {

    private final SamosaConsumer<Long, String> samosaConsumer;
    private final AtomicBoolean isStarted;
    final static Logger logger = Logger.getLogger(ConsumerTask.class);
    List<String> initialTopics;

    public ConsumerTask(SamosaConsumer<Long, String> samosaConsumer, List<String> initialTopics) {
        this.samosaConsumer = samosaConsumer;
        this.initialTopics = initialTopics;
        isStarted = new AtomicBoolean(true);
    }

    @Override
    public void run() {
        int noRecordsCount = 0;
        int giveUp = 10;
        Long pollTimeout = 1000L;
        while (isStarted.get()) {
            final ConsumerRecords<Long, String> consumerRecords =
                    samosaConsumer.dynamicPoll(pollTimeout);

            if (consumerRecords.count()==0) {
                logger.error("No records received.");
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }
            logger.debug(String.format("Total Consumer Records: [%d]", consumerRecords.count()));
            consumerRecords.forEach(samosaConsumer::updateMetrics);

            //De-comment if needed for verbose output.
//            consumerRecords.forEach(record ->
//                    System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
//                            record.key(), record.value(),
//                            record.partition(), record.offset())
//            );

            samosaConsumer.commitAsync();
        }
        samosaConsumer.close();
    }

    public void start() {
        isStarted.set(true);
        samosaConsumer.subscribe(initialTopics);
    }

    public void stop() {
        isStarted.set(false);
    }
}
