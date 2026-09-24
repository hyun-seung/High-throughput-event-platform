package event.delivery.dispatch.external.client;

/** Observed provider outcome, not authorization to re-send or a durable retry decision. */
public class ProviderFailureException extends IllegalStateException {
    public enum Kind {
        RETRY_1S, RETRY_10S, FALLBACK_REQUIRED, PERMANENT_REJECTION,
        NO_RESPONSE, HTTP_ERROR, INVALID_RESPONSE
    }

    private final Kind kind;

    public ProviderFailureException(Kind kind) {
        // Do not include untrusted response bodies or parser exceptions in Kafka error logs.
        super("Provider outcome: " + kind);
        this.kind = kind;
    }

    public Kind kind() {
        return kind;
    }
}
