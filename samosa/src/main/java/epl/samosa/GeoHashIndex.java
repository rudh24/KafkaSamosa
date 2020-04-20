package epl.samosa;

import ch.hsr.geohash.GeoHash;
import org.apache.commons.collections4.Trie;
import org.apache.commons.collections4.trie.PatriciaTrie;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.util.*;


public class GeoHashIndex <T>{
    Logger log = Logger.getLogger(GeoHashIndex.class);

    Trie<String, T> spatialIndex; //hash->value
    int hashPrecision;

    public GeoHashIndex(Properties properties){
        spatialIndex = new PatriciaTrie<>();
        hashPrecision = 8;
        if(properties.containsKey("maxCharPrecision")){
            hashPrecision = (int)properties.get("maxCharPrecision");
        }
    }

    public void addIndex(double x, double y, T value) {
        GeoHash gh = GeoHash.withCharacterPrecision(x, y, hashPrecision);
        String key = gh.toBase32();
        spatialIndex.put(key, value);
    }

    public SortedMap<String, T> getNearestNeighbors(double x, double y){
        GeoHash gh = GeoHash.withCharacterPrecision(x, y, hashPrecision);
        String prefix = gh.toBase32();
        SortedMap<String, T> prefixMap;
        do {
            prefixMap = spatialIndex.prefixMap(prefix);
            prefix = StringUtils.chop(prefix);
        } while( prefixMap.isEmpty());

        return prefixMap;
    }

//    public String getStringValue(double x, double y){
//        return GeoHash.withCharacterPrecision(x, y, hashPrecision).toBase32();
//    }
//
//    public long getIndexSize(){
//        return spatialIndex.size();
//    }
//
//    public void createIndex(double minX, double minY, double maxX, double maxY, double incr){
//
//        for(double x = minX; x <= maxX; x += incr){
//            for(double y = minY; y <= maxY; y += incr){
//                GeoHash gh = GeoHash.withCharacterPrecision(x, y, hashPrecision);
//                String key = gh.toBase32();
//                //GeoHash[] neighbors = gh.getAdjacent();
//                List<String> vals = new ArrayList<>();
//                /*for(int i =0; i < neighbors.length; ++i){
//                    vals.add(neighbors[i].toBase32());
//                }*/
//            }
//        }
//        //System.out.println("index size = " + spatialIndex.size());
//        log.info("index size = " + spatialIndex.size());
//    }


}