package epl.samosa.client;

public interface SubscriptionChangedCallback<T>{

    void onSubscriptionChange(T oldVal, T newVal);
}