package epl.samosa.taster;

import com.fasterxml.jackson.databind.ObjectMapper;
import epl.pubsub.location.indexperf.Index;
import epl.pubsub.location.indexperf.IndexFactory;
import epl.samosa.client.*;
import epl.samosa.location.*;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class TestRunner {
    private final Config config;
    private final StringBuilder payload = new StringBuilder();
    private final Index index;

    ExecutorService consumerExecutor;
    ExecutorService producerExecutor;
    ExecutorService locationManagerExecutor;

    private static final Logger log = LoggerFactory.getLogger(TestRunner.class);


    public TestRunner(Config config){
        this.config = config;
        IndexConfig indexConfig = config.indexConfig;

        try{
            BufferedReader br = new BufferedReader(new FileReader(config.payloadFile));

            String st;
            while ((st = br.readLine()) != null){
                payload.append(st);
            }
        } catch(IOException ex){
            ex.printStackTrace();
        }

        Properties props = new Properties();
        if(indexConfig.indexType.equals("GEOHASH")){
            index = IndexFactory.getInitializedIndex(indexConfig.minX, indexConfig.minY, indexConfig.maxX, indexConfig.maxY, indexConfig.blockSize, IndexFactory.IndexType.GEOHASH, props);
        }
        else {
            index = IndexFactory.getInitializedIndex(indexConfig.minX, indexConfig.minY, indexConfig.maxX, indexConfig.maxY, indexConfig.blockSize, IndexFactory.IndexType.RTREE, props);

        }
        log.info("created index");

        consumerExecutor = Executors.newFixedThreadPool(config.numConsumers);
        producerExecutor = Executors.newFixedThreadPool(config.numProducers);
        locationManagerExecutor = Executors.newFixedThreadPool(config.numProducers + config.numConsumers);
    }

    private static SamosaProducer<Long, String> createProducer(final Config config) {
        //Probably specify path for karka-props.yaml via config
        String bootstrapServers =
                "localhost:9092,localhost:9093,localhost:9094";
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        return new SamosaProducer<>(props);
    }

    public List<SamosaProducer<Long, String>> getProducers(int numProducers){

        List<SamosaProducer<Long,String>> producers = new ArrayList<>();
        for(int i = 0; i < numProducers; ++i){
            producers.add(createProducer(config));
        }
        return producers;
    }

    private static SamosaConsumer<Long, String> createConsumer(final Config config) {
        //Probably specify path for karka-props.yaml via config
        String bootstrapServers =
                "localhost:9092,localhost:9093,localhost:9094";
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "KafkaExampleConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        return new SamosaConsumer<>(props);
    }

    public List<SamosaConsumer<Long, String>> getConsumers(int numConsumers){
        List<SamosaConsumer<Long, String>> consumers = new ArrayList<>();
        for(int i = 0; i < numConsumers; ++i){
            consumers.add(createConsumer(config));
        }
        return consumers;
    }

    public List<ConsumerTask> getConsumerTasks(List<SamosaConsumer<Long, String>> consumers){
        List<ConsumerTask> consumerTasks = new ArrayList<>();
        List<String> topics = new ArrayList<>();
        String topicStr = "init_test";
        topics.add(topicStr);
        for (SamosaConsumer<Long, String> consumer : consumers) {
            ConsumerTask consumerTask = new ConsumerTask(consumer, topics);
            consumerTask.start();
            consumerExecutor.execute(consumerTask);
            consumerTasks.add(consumerTask);
        }
        return consumerTasks;
    }

    public List<ProducerTask> getProducerTasks(List<SamosaProducer<Long, String>> producers){
        List<ProducerTask> producerTasks = new ArrayList<>();
        String topic = "init_test";
        for (SamosaProducer<Long, String> producer : producers) {
            ProducerTask producerTask = new ProducerTask(producer, payload.toString(), 1, topic);
            producerTask.start();
            producerExecutor.execute(producerTask);
            producerTasks.add(producerTask);
        }
        return producerTasks;
    }

    public List<LocationSubscriptionHandlerSingleTopicImpl> getProducerLocationHandlers(List<SamosaProducer<Long, String>> producers, Index index){
        List<LocationSubscriptionHandlerSingleTopicImpl> handlers = new ArrayList<>();
        for (SamosaProducer<Long, String> producer : producers) {
            LocationSubscriptionHandlerSingleTopicImpl handler = new LocationSubscriptionHandlerSingleTopicImpl(index);
            handler.initSubscriptionChangedCallback(producer);
            handlers.add(handler);
        }
        return handlers;
    }

    public List<LocationSubscriptionHandlerMultiTopicImpl> getConsumerLocationHandlers(List<SamosaConsumer<Long, String>> consumers, Index index){
        List<LocationSubscriptionHandlerMultiTopicImpl> handlers = new ArrayList<>();
        for (SamosaConsumer<Long, String> consumer : consumers) {
            LocationSubscriptionHandlerMultiTopicImpl handler = new LocationSubscriptionHandlerMultiTopicImpl(index);
            handler.initSubscriptionChangedCallback(consumer);
            handlers.add(handler);
        }
        return handlers;
    }

    public List<LocationManager> getLocationManagersSingleTopic(List<LocationSubscriptionHandlerSingleTopicImpl> locationHandlers, List<String> trajectoryFiles){
        List<LocationManager> locationManagers = new ArrayList<>();
        for(int i = 0; i < locationHandlers.size(); ++i){
            LocationManagerImpl lm = new LocationManagerImpl(config.locationChangeInterval, trajectoryFiles.get(i));
            lm.initManager(locationHandlers.get(i));
            lm.start();
            locationManagerExecutor.execute(lm);
            locationManagers.add(lm);
        }
        return locationManagers;
    }

    public List<LocationManager> getLocationManagersMultiTopic(List<LocationSubscriptionHandlerMultiTopicImpl> locationHandlers, List<String> trajectoryFiles){
        List<LocationManager> locationManagers = new ArrayList<>();
        for(int i = 0; i < locationHandlers.size(); ++i){
            LocationManagerImpl lm = new LocationManagerImpl(config.locationChangeInterval, trajectoryFiles.get(i));
            lm.initManager(locationHandlers.get(i));
            locationManagerExecutor.execute(lm);
            locationManagers.add(lm);
        }
        return locationManagers;
    }


    public void runTest(){
        if(config.numProducers != config.producerTrajectoryFiles.size()){
            System.out.println("Number of producers and trajectories don't match");
            return;
        }
        if(config.numConsumers != config.consumerTrajectoryFiles.size()){
            System.out.println("Number of consumers and trajectories don't match");
            return;
        }
        StopWatch sw = new StopWatch();
        sw.start();
        List<SamosaProducer<Long, String>> producers = getProducers(config.numProducers);
        sw.stop();
        log.info("created {} producers in {} ms", config.numProducers, sw.getTime());
        sw.reset();
        List<ProducerTask> producerTasks = getProducerTasks(producers);
        log.info("created producer tasks");

        sw.start();
        List<SamosaConsumer<Long, String>> consumers = getConsumers(config.numConsumers);
        sw.stop();
        log.info("created {} consumers in {} ms", config.numConsumers, sw.getTime());

        List<ConsumerTask> consumerTasks = getConsumerTasks(consumers);
        log.info("created consumer tasks");
        List<LocationSubscriptionHandlerSingleTopicImpl> producerHandlers = getProducerLocationHandlers(producers, index);
        log.info("created subscription handlers for producers");

        List<LocationSubscriptionHandlerMultiTopicImpl> consumerHandlers = getConsumerLocationHandlers(consumers, index);
        log.info("created subscription handlers for consumers");

        List<LocationManager> producerLocationManagers = getLocationManagersSingleTopic(producerHandlers, config.producerTrajectoryFiles);
        log.info("Created producer location managers");

        List<LocationManager> consumerLocationManagers = getLocationManagersMultiTopic(consumerHandlers, config.consumerTrajectoryFiles);
        log.info("Created consumer location managers");

        try {
            Thread.sleep(10000);
        } catch(InterruptedException e){
            e.printStackTrace();
        }

        for (ProducerTask producerTask : producerTasks) {
            producerTask.stop();
        }
        for (LocationManager producerLocationManager : producerLocationManagers) {
            producerLocationManager.stop();
        }
        for (ConsumerTask consumerTask : consumerTasks) {
            consumerTask.stop();
        }
        for (LocationManager consumerLocationManager : consumerLocationManagers) {
            consumerLocationManager.stop();
        }
        consumerExecutor.shutdown();
        producerExecutor.shutdown();
        locationManagerExecutor.shutdown();

        class PubSubMetrics{
            public List<ConsumerMetrics> consumerMetrics;
            public List<ProducerMetrics> producerMetrics;
            public int numConsumers;
            public int numProducers;
        }

        PubSubMetrics metrics = new PubSubMetrics();
        metrics.producerMetrics = new ArrayList<>();
        metrics.consumerMetrics = new ArrayList<>();
        metrics.numConsumers = consumers.size();
        metrics.numProducers = producers.size();
        for (SamosaConsumer<Long, String> consumer : consumers) {
            metrics.consumerMetrics.add(consumer.getConsumerMetrics());
        }
        for (SamosaProducer<Long, String> producer : producers) {
            metrics.producerMetrics.add(producer.getProducerMetrics());
        }
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.writeValue(new File(config.outputFile), metrics);
        }catch(IOException ex){
            ex.printStackTrace();
        }
    }
}