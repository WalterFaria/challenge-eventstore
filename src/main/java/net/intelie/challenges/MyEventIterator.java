package net.intelie.challenges;
import java.util.Iterator;
import java.util.SortedMap;

/**
 * An iterator over an event collection.
 *
 * This class receives the previously sliced Map, containing now only the events of interest.
 * The keys of this Map can be converted to a Set which can generate an Iterator.
 * This Iterator has some pre-built methods (hasNext() and next()) that will help us to create
 * our own Iterator.
 * Since ConcurrentSkipListMap is thread-safe we can use the Iterator's method remove() without
 * throwing a ConcurrentModificationException.
 */
public class MyEventIterator<T> implements EventIterator {

    private boolean calledOnce = false; // flag that indicates the result of hasNext()
    private Event currentEvent = null;  // event to be returned by the method current()
    private SortedMap<Long, Event> internalMap; // map selected by the method EventStore.query to be iterated over
    private Iterator it;    // our iterator that will receive the methods from the keySet iterator generated from the map

    public MyEventIterator(SortedMap<Long, Event> internalMap) {
        // initializing our attributes
        this.internalMap = internalMap;
        this.it = internalMap.keySet().iterator();
    }

    /**
     * Move the iterator to the next event, if any.
     *
     * @return false if the iterator has reached the end, true otherwise.
     */
    public boolean moveNext() {
        if (it.hasNext()) {                                 // if there is a next element
            calledOnce = true;                              // the flag is changed
            long eventTimestamp = (Long) it.next();         // the iterator is moved and then its key is read
            currentEvent = internalMap.get(eventTimestamp); // the event is stored and this is now the current event
            return true;
        }
        calledOnce = false;     // if there is no next element the flag is changed back to its original state
        return false;
    }

    public Event current() {
        if (!calledOnce) {      // moveNext() must be called at least once to initialize the Iterator
            throw new IllegalStateException("Method moveNext() was never called.");
        }
        return currentEvent;
    }

    public void remove() {
        if (!calledOnce) {      // if there is no current event there is nothing to be removed
            throw new IllegalStateException("Method moveNext() was never called.");
        }
        // this is where the choice of ConcurrentSkipListMap makes a huge difference and thread-safety is guaranteed
        it.remove();
    }

    @Override
    public void close() throws Exception {

    }
}
