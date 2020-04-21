package epl.samosa;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.HashMap;

public class StaticTopics {
    public  HashMap<String, Pair<Double, Double>> coordinateMap;
    final static org.apache.log4j.Logger logger =
        org.apache.log4j.Logger.getLogger(StaticTopics.class);
    public StaticTopics() {
        try {
            Class<?> c = getClass();
            logger.error("In Static Topics: "+c.getName());
            BufferedReader fileReader =
                new BufferedReader(
                    new InputStreamReader( getClass().getResourceAsStream("/FogNodesLocation.json")));

            JSONObject jo = (JSONObject) new JSONParser().parse(fileReader);
            JSONObject nodes = (JSONObject) jo.get("nodes");

            coordinateMap = new HashMap<>();

            nodes.keySet().forEach(keyStr -> {
                JSONArray array = (JSONArray) nodes.get(keyStr);
                Pair<Double, Double> coordinate = new MutablePair<>((Double)array.get(0),
                        (Double)array.get(1));
                coordinateMap.put((String) keyStr, coordinate);
            });

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
