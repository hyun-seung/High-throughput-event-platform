package event.delivery.result;

import event.delivery.result.notification.NotificationProperties;
import org.junit.jupiter.api.Test;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class NotificationPropertiesTest {
    @Test void retryDelayIsCappedAndUsesInitialAttemptAsOne() {
        var p = new NotificationProperties(false, 4, 100, Duration.ofSeconds(3), Duration.ofSeconds(30), Duration.ofSeconds(1), Duration.ofSeconds(60), 262144, Map.of());
        assertEquals(Duration.ofSeconds(1), p.retryAfter(1));
        assertEquals(Duration.ofSeconds(2), p.retryAfter(2));
        assertEquals(Duration.ofSeconds(60), p.retryAfter(7));
        assertEquals(Duration.ofSeconds(60), p.retryAfter(21));
    }
    @Test void rejectsUnsafeDestinationAndInsufficientLease() {
        for (String url : new String[]{"http://example.com/results", "https://user:pass@example.com/results", "file:///tmp/results", "https://example.com/results#fragment"})
            assertThrows(IllegalArgumentException.class, () -> new NotificationProperties.Destination(URI.create(url), NotificationTestServer.TOKEN));
        assertThrows(IllegalArgumentException.class, () -> new NotificationProperties.Destination(URI.create("https://example.com"), "short"));
        assertThrows(IllegalArgumentException.class, () -> new NotificationProperties(false, 4, 100, Duration.ofSeconds(3), Duration.ofSeconds(3), Duration.ofSeconds(1), Duration.ofSeconds(60), 262144, Map.of()));
        assertThrows(IllegalArgumentException.class, () -> new NotificationProperties(true, 4, 100, Duration.ofSeconds(3), Duration.ofSeconds(30), Duration.ofSeconds(1), Duration.ofSeconds(60), 262144, Map.of()));
    }
}
