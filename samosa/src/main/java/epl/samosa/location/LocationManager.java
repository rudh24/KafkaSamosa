package epl.samosa.location;

import epl.pubsub.location.indexperf.Index;

public interface LocationManager {
    void monitorLocation();

    void initManager(LocationChangedCallback cb);

    void start();

    void stop();
}