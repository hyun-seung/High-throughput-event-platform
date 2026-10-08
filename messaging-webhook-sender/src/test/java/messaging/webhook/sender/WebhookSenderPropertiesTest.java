package messaging.webhook.sender;

import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WebhookSenderPropertiesTest {
    @Test
    void externalHttpRequiresExplicitLocalTestOptIn() {
        URI mock = URI.create("http://mock:18093/hook");
        assertThrows(IllegalArgumentException.class, () -> new WebhookSenderProperties.Destination(mock, false));
        assertEquals(mock, new WebhookSenderProperties.Destination(mock, true).url());
        assertEquals(URI.create("http://localhost:18093/hook"),
                new WebhookSenderProperties.Destination(URI.create("http://localhost:18093/hook"), false).url());
    }

}
