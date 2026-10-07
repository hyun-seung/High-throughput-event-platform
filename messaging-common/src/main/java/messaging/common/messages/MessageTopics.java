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
    /** Final customer notification command consumed by MSG-WEBHOOK-SENDER. */
    public static final String WEBHOOK_SEND = "WEBHOOK-SEND";
    public static final String TCP_SEND = "message.tcp.requested.v1";

    public static String httpSend(HttpCarrier carrier) {
        return switch (carrier) {
            case SKT -> SKT_HTTP_SEND;
            case KT -> KT_HTTP_SEND;
            case LGU -> LGU_HTTP_SEND;
        };
    }

    private MessageTopics() { }
}
