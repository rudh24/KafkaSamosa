package epl.samosa.taster;

import java.io.File;
import java.io.IOException;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.apache.log4j.Logger;

public class Main {
    final static Logger logger = Logger.getLogger(Main.class);
    static class Arguments {

        @Parameter(names = {"-h", "--help"}, description = "Help message", help = true)
        boolean help;

        @Parameter(names = {"-c",
                "--config"}, description = "YAML Config File", required = true)
        public String configFile;
    }

    public static void main(String[] args) {
        final Arguments arguments = new Arguments();
        JCommander jc = new JCommander(arguments);
        jc.setProgramName("samosa-taster");

        try {
            jc.parse(args);
        } catch (ParameterException e) {
            System.err.println(e.getMessage());
            jc.usage();
            System.exit(-1);
        }

        if (arguments.help) {
            jc.usage();
            System.exit(-1);
        }


        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.findAndRegisterModules();

        try {
            logger.debug(arguments.configFile);
            Config config = mapper.readValue(new File(arguments.configFile), Config.class);
            logger.info("Creating Test Runner");
            TestRunner tr = new TestRunner(config);
            tr.runTest();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.exit(0);
    }
}
