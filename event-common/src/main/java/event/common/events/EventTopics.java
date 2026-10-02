package event.common.events;

public final class EventTopics {
    public static final String HTTP_REQUESTED = "event.http.requested.v1";
    public static final String HTTP_OUTCOME = "event.http.outcome.v1";
    public static final String HTTP_RETRY = "event.http.retry.v1";
    public static final String TCP_REQUESTED = "event.tcp.requested.v1";
    public static final String TCP_OUTCOME = "event.tcp.outcome.v1";
    public static final String TCP_RETRY = "event.tcp.retry.v1";
    public static final String FINALIZED = "event.finalized.v1";

    private EventTopics() { }
}
