package messaging.common.messages;

/** Shared 1st-send HTTP carrier error codes in the 60000 range. */
public final class PrimaryHttpFailureCodes {
    /** The recipient number does not belong to the carrier that received this invocation. */
    public static final int NOT_OUR_CARRIER = 66001;

    /** The carrier rejected this invocation because its TPS limit was exceeded. */
    public static final int TPS_EXCEEDED = 66002;

    private PrimaryHttpFailureCodes() { }
}
