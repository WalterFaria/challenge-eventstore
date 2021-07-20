package net.intelie.challenges;

import org.junit.Test;
import java.security.InvalidParameterException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class EventTest {
    @Test
    public void thisIsAWarning() throws Exception {
        Event event = new Event("some_type", 123L);

        //THIS IS A WARNING:
        //Some of us (not everyone) are coverage freaks.
        assertEquals(123L, event.timestamp());
        assertEquals("some_type", event.type());
    }

    @Test
    public void simpleInsertion() {
        // making certain that the event was inserted
        MyEventStore myStore = new MyEventStore();
        Event ev = new Event("test", 100);
        myStore.insert(ev);
        assertEquals(1, myStore.sizeof());
    }

    @Test
    public void removeAllFromType() {
        MyEventStore myStore = new MyEventStore();
        myStore.insert(new Event("A", 0));
        myStore.insert(new Event("B", 1));
        myStore.insert(new Event("A", 2));
        myStore.insert(new Event("A", 3));
        myStore.insert(new Event("A", 4));
        myStore.insert(new Event("C", 5));
        // 6 events were added
        assertEquals(6, myStore.sizeof());
        // 4 events are of type A
        // if they are removed we must end up with 2 events
        myStore.removeAll("A");
        assertEquals(2, myStore.sizeof());
    }

    @Test
    public void removeDuringIteration() {
        // making sure that the remove() method of the iterator works
        MyEventStore myStore = new MyEventStore();
        myStore.insert(new Event("A", 0));
        myStore.insert(new Event("B", 1));
        myStore.insert(new Event("A", 2));
        myStore.insert(new Event("B", 3));
        myStore.insert(new Event("B", 4));
        myStore.insert(new Event("C", 5));
        myStore.insert(new Event("B", 6));
        myStore.insert(new Event("B", 7));
        myStore.insert(new Event("B", 8));
        /* there are nine elements in storage
         * "all" the even timestamps with type "B" will be removed
         * since the endTime parameter is exclusive, there are actually five events in the iterator, not all six of type B
         * from these six events, two have even timestamps (4 and 6)
         * therefore, two events of type "B" will be removed, leaving the EventStore with 7 elements in total
         */
        MyEventIterator<Event> it = myStore.query("B", 0, 8);
        while (it.moveNext()) {
            if (it.current().timestamp() % 2 == 0)
                it.remove();
        }
        assertEquals(7, myStore.sizeof());

    }

    @Test
    public void notValidQueryParameters() {
        // confirming that the Exceptions are thrown when invalid parameters are passed to the query() method
        MyEventStore myStore = new MyEventStore();
        myStore.insert(new Event("A", 0));
        myStore.insert(new Event("A", 10));
        myStore.insert(new Event("A", 20));

        // querying for a non-existent type
        assertThrows(InvalidParameterException.class, () -> {
            myStore.query("B", 0, 20);
        });

        // querying with an endTime bigger than the startTime
        assertThrows(InvalidParameterException.class, () -> {
            myStore.query("A", 20, 0);
        });
    }

    @Test
    public void throwExceptionAtIterator() {
        // confirming that the iterator methods throw exceptions if moveNext() is not called first
        MyEventStore myStore = new MyEventStore();
        myStore.insert(new Event("A", 0));
        myStore.insert(new Event("A", 10));
        myStore.insert(new Event("A", 20));
        myStore.insert(new Event("A", 30));
        MyEventIterator<Event> it = myStore.query("A", 0, 30);

        // calling current() before moveNext()
        assertThrows(IllegalStateException.class, it::current);

        // calling remove() before moveNext()
        assertThrows(IllegalStateException.class, it::remove);

    }

    @Test
    public void threadTest() {
        MyEventStore myTest = new MyEventStore();
        for (int i = 0; i < 21; i++)
            myTest.insert(new Event("B", i));
        myTest.insert(new Event("C", 0));
        myTest.insert(new Event("D", 0));
        myTest.insert(new Event("E", 0));
        myTest.insert(new Event("F", 0));
        // 21 events (timestamps 0 to 20) of type B were added
        // 4 events (timestamp 0), each with a different type, were added

        // thread 1 adds 100 events of type A
        Thread t1 = new Thread(() -> {
            for (int i = 0; i < 100; i++)
                myTest.insert(new Event("A", i));
        });

        // thread 2 removes all events of type B with even timestamps between 0 and 20
        // but 20 is not included (endTime parameter of method query is exclusive)
        // therefore, timestamps to be removed: 0, 2, 4, ..., 18
        // 10 events in total to be removed
        Thread t2 = new Thread( () -> {
            try (MyEventIterator<Event> it = myTest.query("B", 0, 20)) {
                while (it.moveNext())
                    if (it.current().timestamp() % 2 == 0)
                        it.remove();
            } catch(Exception e) {
                e.printStackTrace();
            }
        });

        t1.start();
        t2.start();
        try {
            t1.join();
            t2.join();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 25 events were added at the beginning
        // 100 events were added by thread 1
        // 10 events were removed by thread 2
        // we must have 115 events at the end
        assertEquals(115, myTest.sizeof());
    }
}