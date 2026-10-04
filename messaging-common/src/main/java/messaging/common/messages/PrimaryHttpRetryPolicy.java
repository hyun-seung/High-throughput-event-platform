package messaging.common.messages;

import java.time.Duration;

/** Shared retry timing for first-send HTTP carrier results. */
public final class PrimaryHttpRetryPolicy {
    /** Earliest retry after carrier TPS_EXCEEDED (66002). */
    public static final Duration TPS_RETRY_DELAY = Duration.ofMinutes(1);

    /** Retries after the first send to the same carrier for 66002. */
    public static final int TPS_MAX_RETRIES = 3;

    private PrimaryHttpRetryPolicy() { }
}
