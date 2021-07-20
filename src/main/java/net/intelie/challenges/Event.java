package net.intelie.challenges;

/**
 * The only change that I have made in this class is the return
 * of the type() and timestamp() methods.
 * They went from "return attribute" to "return this.attribute".
 * No other changes changes were necessary.
 */
public class Event {
    private final String type;
    private final long timestamp;

    public Event(String type, long timestamp) {
        this.type = type;
        this.timestamp = timestamp;
    }

    public String type() {
        return this.type;
    }

    public long timestamp() {
        return this.timestamp;
    }
}
