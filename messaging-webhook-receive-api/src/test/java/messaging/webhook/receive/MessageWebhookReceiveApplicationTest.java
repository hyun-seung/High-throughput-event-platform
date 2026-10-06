package messaging.webhook.receive;

import messaging.webhook.receive.config.MessageWebhookProperties;
import messaging.webhook.receive.kafka.KafkaMessageWebhookPublisher;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK,
        properties = {"spring.kafka.admin.auto-create=false", "message.webhook.skt-secret="})
class MessageWebhookReceiveApplicationTest {
    @Autowired MessageWebhookProperties properties;
    @Autowired KafkaMessageWebhookPublisher publisher;

    @Test
    void independentWebhookApplicationWiresWithoutTheLegacyReceiptModule() {
        assertEquals("", properties.secret(messaging.common.messages.HttpCarrier.SKT));
        assertNotNull(publisher);
    }
}
