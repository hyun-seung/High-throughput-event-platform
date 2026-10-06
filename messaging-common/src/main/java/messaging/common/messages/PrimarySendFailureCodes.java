package messaging.common.messages;

/** Messaging-service failure reasons for the entire first-send stage. */
public final class PrimarySendFailureCodes {
    /** The carrier kept returning 66002 until the allowed first-send retry count was exhausted. */
    public static final int TPS_RETRY_EXHAUSTED = 40001;

    /** SKT, KT and LGU all returned 66001 for the same message. */
    public static final int NO_MATCHING_CARRIER = 40002;

    /** No HTTP response after the first call and three delayed retries. */
    public static final int NO_RESPONSE_RETRY_EXHAUSTED = 40003;

    private PrimarySendFailureCodes() { }
}
