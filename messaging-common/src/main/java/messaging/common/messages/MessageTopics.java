package messaging.common.messages;

public final class MessageTopics {
    /** Admission fact produced by MSG-RECEIVE-API and consumed by PRE-SEND-MANAGER. */
    public static final String RECEIVED = "message.received.v1";
    public static final String SKT_HTTP_SEND = "message.skt.http.send.v1";
    public static final String KT_HTTP_SEND = "message.kt.http.send.v1";
    public static final String LGU_HTTP_SEND = "message.lgu.http.send.v1";
    /** Pre-send failures, webhook batches, HTTP failures/timeouts and TCP responses for MSG-RESULT-MANAGER. */
    public static final String MSG_RESULT = "MSG_RESULT";
    /** Final business result produced by MSG-RESULT-MANAGER. */
    public static final String MSG_RESULT_FINALIZED = "MSG-RESULT-FINALIZED";

    public static String httpSend(HttpCarrier carrier) {
        return switch (carrier) {
            case SKT -> SKT_HTTP_SEND;
            case KT -> KT_HTTP_SEND;
            case LGU -> LGU_HTTP_SEND;
        };
    }

    /** Legacy direct-to-sender topic; the new admission path does not publish here. */
    public static final String HTTP_REQUESTED = "message.http.requested.v1";
    public static final String HTTP_OUTCOME = "message.http.outcome.v1";

    private MessageTopics() { }
}
