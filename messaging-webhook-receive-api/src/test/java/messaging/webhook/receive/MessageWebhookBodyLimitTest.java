package messaging.webhook.receive;

import messaging.webhook.receive.api.MessageWebhookBodyLimit;
import messaging.webhook.receive.api.MessageWebhookController;
import org.junit.jupiter.api.Test;
import org.springframework.core.MethodParameter;
import org.springframework.mock.http.MockHttpInputMessage;
import org.springframework.web.server.ResponseStatusException;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

class MessageWebhookBodyLimitTest {
    @Test
    void chunkedRequestOverLimitIsRejectedBeforeDeserialization() throws Exception {
        Method receive = MessageWebhookController.class.getMethod("receive",
                messaging.common.messages.HttpCarrier.class, java.util.List.class);
        MethodParameter parameter = new MethodParameter(receive, 1);
        var input = new MockHttpInputMessage(new byte[MessageWebhookBodyLimit.MAX_BYTES + 1]);
        var limit = new MessageWebhookBodyLimit();

        assertTrue(limit.supports(parameter, parameter.getGenericParameterType(), null));
        assertThrows(ResponseStatusException.class,
                () -> limit.beforeBodyRead(input, parameter, parameter.getGenericParameterType(), null));
    }
}
