package event.common.events;

public final class EventTopics {
    /** Admission fact produced by EVENT-RECEIVE-API and consumed by PRE-SEND-MANAGER. */
    public static final String RECEIVED = "event.received.v1";
    public static final String SKT_HTTP_SEND = "event.skt.http.send.v1";
    public static final String KT_HTTP_SEND = "event.kt.http.send.v1";
    public static final String LGU_HTTP_SEND = "event.lgu.http.send.v1";
    /** Unified immediate-response and webhook result topic for the new send path. */
    public static final String MSG_RESULT = "MSG_RESULT";

    public static String httpSend(HttpCarrier carrier) {
        return switch (carrier) {
            case SKT -> SKT_HTTP_SEND;
            case KT -> KT_HTTP_SEND;
            case LGU -> LGU_HTTP_SEND;
        };
    }

    /** Legacy direct-to-sender topic; the new admission path does not publish here. */
    public static final String HTTP_REQUESTED = "event.http.requested.v1";
    public static final String HTTP_OUTCOME = "event.http.outcome.v1";
    public static final String HTTP_RETRY = "event.http.retry.v1";
    public static final String TCP_REQUESTED = "event.tcp.requested.v1";
    public static final String TCP_OUTCOME = "event.tcp.outcome.v1";
    public static final String TCP_RETRY = "event.tcp.retry.v1";
    public static final String FINALIZED = "event.finalized.v1";

    private EventTopics() { }
}
