package epl.samosa.location;

import epl.samosa.client.SubscriptionChangedCallback;

public interface LocationSubscriptionHandler<T> extends LocationChangedCallback {

    void initSubscriptionChangedCallback(SubscriptionChangedCallback<T> callback);

}