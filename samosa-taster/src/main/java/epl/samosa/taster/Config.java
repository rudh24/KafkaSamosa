package epl.samosa.taster;

import java.util.List;

public class Config {
    public int numProducers;
    public int numConsumers;
    public int numPartitionsPerTopic;
    public IndexConfig indexConfig;
    public int locationChangeInterval;
    public String payloadFile;
    public String configFile;
    public List<String> producerTrajectoryFiles;
    public List<String> consumerTrajectoryFiles;
    public int testDurationSeconds;
    public String outputFile;
}
