package epl.samosa.location;

public interface LocationChangedCallback{

    void onLocationChange(Location oldLocation, Location newLocation);
}