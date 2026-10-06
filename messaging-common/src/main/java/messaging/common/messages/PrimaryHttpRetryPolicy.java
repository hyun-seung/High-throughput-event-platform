package messaging.common.messages;

import java.time.Duration;

/** Shared retry timing for first-send HTTP carrier results. */
public final class PrimaryHttpRetryPolicy {
    /** Earliest retry after carrier TPS_EXCEEDED (66002). */
    public static final Duration TPS_RETRY_DELAY = Duration.ofMinutes(1);

    /** Retries after the first send to the same carrier for 66002. */
    public static final int TPS_MAX_RETRIES = 3;

    /** Wait at most five seconds for the first-send HTTP response. */
    public static final Duration RESPONSE_TIMEOUT = Duration.ofSeconds(5);

    /** Retry an HTTP call with no response one minute after its timeout. */
    public static final Duration NO_RESPONSE_RETRY_DELAY = Duration.ofMinutes(1);

    /** Retries after the first send when no HTTP response arrives. */
    public static final int NO_RESPONSE_MAX_RETRIES = 3;

    private PrimaryHttpRetryPolicy() { }
}
