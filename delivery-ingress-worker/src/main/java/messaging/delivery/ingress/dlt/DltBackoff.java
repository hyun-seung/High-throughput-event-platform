package messaging.delivery.ingress.dlt;

import messaging.common.recovery.FailureBackoff;
import messaging.common.recovery.StorageFailure;
import java.time.Duration;

final class DltBackoff {
    private DltBackoff() { }
    static FailureBackoff defaults() { return new FailureBackoff(Duration.ofSeconds(1), Duration.ofSeconds(30)); }
    static boolean unavailable(Throwable failure) {
        if (StorageFailure.unavailable(failure)) return true;
        for (int depth = 0; failure != null && depth < 12; depth++, failure = failure.getCause())
            if (failure instanceof org.apache.kafka.common.errors.RetriableException) return true;
        return false;
    }
}
