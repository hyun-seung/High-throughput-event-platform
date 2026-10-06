package messaging.webhook.receive;

import messaging.common.messages.HttpCarrier;
import messaging.webhook.receive.config.MessageWebhookProperties;
import messaging.webhook.receive.security.MessageWebhookAuthenticationFilter;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.core.context.SecurityContextHolder;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class MessageWebhookAuthenticationFilterTest {
    private static final String SKT = "s".repeat(32);
    private static final String KT = "k".repeat(32);
    private final MessageWebhookAuthenticationFilter filter = new MessageWebhookAuthenticationFilter(
            new MessageWebhookProperties(SKT, KT, "l".repeat(32)));

    @Test
    void matchingCarrierSecretSetsAuthenticatedCarrier() throws Exception {
        var request = new MockHttpServletRequest("POST", "/api/v1/message-webhooks/skt");
        request.setServletPath("/api/v1/message-webhooks/skt");
        request.addHeader("Authorization", "Bearer " + SKT);
        var response = new MockHttpServletResponse();
        var observed = new AtomicReference<Object>();
        try {
            filter.doFilter(request, response, (req, res) -> observed.set(
                    SecurityContextHolder.getContext().getAuthentication().getPrincipal()));
            assertEquals(HttpCarrier.SKT, observed.get());
        } finally {
            SecurityContextHolder.clearContext();
        }
    }

    @Test
    void wrongOrCrossCarrierSecretCannotReachController() throws Exception {
        var request = new MockHttpServletRequest("POST", "/api/v1/message-webhooks/skt");
        request.setServletPath("/api/v1/message-webhooks/skt");
        request.addHeader("Authorization", "Bearer " + KT);
        var response = new MockHttpServletResponse();
        filter.doFilter(request, response, (req, res) -> fail("Unauthorized request reached controller"));
        assertEquals(401, response.getStatus());
    }
}
