package messaging.reference.cache;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest(properties = {
        "spring.kafka.listener.auto-startup=false",
        "spring.kafka.admin.auto-create=false",
        "spring.main.web-application-type=none"
})
class MessageReferenceCacheApplicationTest {
    @Autowired ReferenceCdcProjector projector;
    @Autowired ReferenceCdcConsumer consumer;

    @Test
    void wiresCdcConsumerAndRedisProjection() {
        assertNotNull(projector);
        assertNotNull(consumer);
    }
}
