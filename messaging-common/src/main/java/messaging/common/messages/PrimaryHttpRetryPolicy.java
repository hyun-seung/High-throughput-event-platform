package messaging.common.messages;

import java.time.Duration;

/** Shared retry timing for first-send HTTP carrier results. */
public final class PrimaryHttpRetryPolicy {
    /** Earliest retry after carrier TPS_EXCEEDED (66002). */
    public static final Duration TPS_RETRY_DELAY = Duration.ofMinutes(1);

    private PrimaryHttpRetryPolicy() { }
}
