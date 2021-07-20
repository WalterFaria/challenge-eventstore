package net.intelie.challenges;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.security.InvalidParameterException;

/**
 * The data structure that I have chosen to use in this challenge is a ConcurrentHashMap
 * with the event types as a key and a ConcurrentSkipListMap containing the events as value.
 * The Map structure is ideal because it allows us to remove all events from a specific type
 * in no time (O(1)). Also, all queries are of a single type. Therefore the usage of a Map
 * requires only a search for the timestamps.
 * ConcurrentHashMap guarantees thread-safe operations.
 *
 * As the values of this Map we have another Map structure: a ConcurrentSkipListMap.
 * This is a sorted Map, meaning that all its entries are sorted (by their keys) during the
 * insertion of a new one. This is why the timestamp was chosen as the key, so that is guaranteed
 * that the events are placed in the order that they have occurred and not in the order they were
 * inserted. This makes the creation of iterators for the queries much easier. Also,
 * ConcurrentSkipListMap has useful methods like ceiling/floor Key.
 * Last, and most importantly, ConcurrentSkipListMap also guarantees thread-safe operations.
 */

public class MyEventStore implements EventStore {

    private Map<String, ConcurrentSkipListMap<Long, Event>> myMap;  // creation of our Map structure

    public MyEventStore() {
        this.myMap = new ConcurrentHashMap<>();     // initialization of our Map structure
    }

    /**
     * Stores an event
     *
     * @param event     Event object
     */
    public void insert (Event event) {
        String type = event.type();
        // if the event is the first of its type to be inserted
        if (!this.myMap.containsKey(type)) {
            // a new map entry is created for this type of event
            this.myMap.put(type, new ConcurrentSkipListMap<Long, Event>());
        }
        // if this event type already exists the event is put in its map
        this.myMap.get(type).put(event.timestamp(), event);
    }

    /**
     * Removes all events of specific type.
     *
     * @param type      The type of the event
     */
    public void removeAll(String type) {
        // no error occurs if the Key type does not exist in the map, no check is needed
        this.myMap.remove(type);
    }

    /**
     * Gets the size of the event store.
     * It is not needed for the class to function properly.
     * I've created this method just for checks, tests, etc.
     *
     * @return the size of the event store.
     *
     */
    public int sizeof() {
        int counter = 0;        // sets the counter to zero
        // iterates through each map corresponding to an event type
        for (ConcurrentSkipListMap<Long, Event> map : this.myMap.values()) {
            counter += map.size();      // sums the particular map size to the overall store size
        }
        return counter;         // returns the overall event store size
    }

    /**
     * Retrieves an iterator for events based on their type and timestamp.
     *
     * @param type      The type we are querying for.
     * @param startTime Start timestamp (inclusive).
     * @param endTime   End timestamp (exclusive).
     * @return An iterator where all its events have same type as
     * {@param type} and timestamp between {@param startTime}
     * (inclusive) and {@param endTime} (exclusive).
     */
    public MyEventIterator<Event> query(String type, long startTime, long endTime) {
        // checks if the parameters make sense, endTime must be bigger than startTime
        if (endTime <= startTime) {
            throw new InvalidParameterException("Parameter endTime must be bigger than the parameter startTime.");
        }
        if (this.myMap.containsKey(type)) {
            // it is possible that the time parameters do not match exactly one of the events timestamps
            // this is where the ceiling/floor Key functions become useful
            // we can find timestamps that are close to the parameters
            // gets the lowest timestamp that is equal to or bigger than startTime
            Long startKey = this.myMap.get(type).ceilingKey(startTime);
            // gets the highest timestamp that is equal to or smaller than startTime
            Long endKey = this.myMap.get(type).floorKey(endTime);
            // gets a submap of our current map containing only the desired events
            SortedMap<Long, Event> eventList = this.myMap.get(type).subMap(startKey, endKey);
            // finally, creates an iterator using the aforementioned submap
            return new MyEventIterator<Event>(eventList);
        }
        // if there is no events of this type there is nothing to return
        throw new InvalidParameterException("Type does not exist.");
    }
}
