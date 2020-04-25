package epl.samosa.location;

import epl.pubsub.location.indexperf.Index;
import epl.samosa.client.SubscriptionChangedCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LocationSubscriptionHandlerSingleTopicImpl implements  LocationSubscriptionHandler<String> {

    private final Index index;
    private SubscriptionChangedCallback<String> callback;
    private static final Logger log = LoggerFactory.getLogger(LocationSubscriptionHandlerSingleTopicImpl.class);

    public LocationSubscriptionHandlerSingleTopicImpl(Index index){
        this.index = index;

    }
    public void initSubscriptionChangedCallback(SubscriptionChangedCallback<String> callback) {
        this.callback = callback;
    }

    @Override
    public void onLocationChange(Location oldLocation, Location newLocation){
        String oldTopic = index.getStringValue(oldLocation.x, oldLocation.y);
        String newTopic = index.getStringValue(newLocation.x, newLocation.y);
        log.info("OLD: {} | NEW: {}", oldTopic, newTopic);
        if(!oldTopic.equals(newTopic)){
            callback.onSubscriptionChange(oldTopic, newTopic);
        }
    }


}