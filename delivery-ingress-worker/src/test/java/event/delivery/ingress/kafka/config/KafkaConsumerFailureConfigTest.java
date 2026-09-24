package event.delivery.ingress.kafka.config;

import event.common.delivery.DeliveryTopics;
import event.delivery.ingress.repository.IdempotencyConflictException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class KafkaConsumerFailureConfigTest {

    private final KafkaConsumerFailureConfig config = new KafkaConsumerFailureConfig();
    private final IngressFailureProperties properties = new IngressFailureProperties(Duration.ZERO, 2L, null);
    private final ConsumerRecord<String, Object> source = new ConsumerRecord<>(
            DeliveryTopics.DELIVERY_REQUESTED, 1, 42, "delivery-1", Map.of("body", "test"));
    private KafkaTemplate<Object, Object> template;
    private DeadLetterPublishingRecoverer recoverer;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        template = mock(KafkaTemplate.class);
        recoverer = config.ingressDeadLetterRecoverer(template, properties);
    }

    @Test
    void recoveryWaitsForDltAckAndPreservesSourceCoordinates() throws Exception {
        var sendStarted = new CountDownLatch(1);
        var ack = new CompletableFuture<SendResult<Object, Object>>();
        when(template.send(any(ProducerRecord.class))).thenAnswer(invocation -> {
            ProducerRecord<Object, Object> output = invocation.getArgument(0);
            assertEquals(DeliveryTopics.DELIVERY_REQUESTED_DLT, output.topic());
            assertEquals(1, output.partition());
            assertEquals(source.key(), output.key());
            assertEquals(source.value(), output.value());
            assertNotNull(output.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC));
            assertNotNull(output.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET));
            sendStarted.countDown();
            return ack;
        });
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var recovery = executor.submit(() -> recoverer.accept(source, new IllegalStateException("failed")));
            try {
                assertTrue(sendStarted.await(5, TimeUnit.SECONDS));
                assertFalse(recovery.isDone(), "Unacknowledged DLT publication is not successful recovery");
            } finally {
                ack.complete(sendResult());
            }
            recovery.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void dltSendFailureDoesNotRecoverTheSource() {
        when(template.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("DLT unavailable")));

        assertThrows(KafkaException.class,
                () -> recoverer.accept(source, new IllegalStateException("processing failed")));
    }

    @Test
    @SuppressWarnings("unchecked")
    void missingDltAckTimesOutWithoutRecovery() {
        ProducerFactory<Object, Object> factory = mock(ProducerFactory.class);
        when(factory.getConfigurationProperties()).thenReturn(Map.of("delivery.timeout.ms", 1));
        when(template.getProducerFactory()).thenReturn(factory);
        when(template.send(any(ProducerRecord.class))).thenReturn(new CompletableFuture<>());
        recoverer.setTimeoutBuffer(0);
        recoverer.setWaitForSendResultTimeout(Duration.ofMillis(10));

        assertThrows(KafkaException.class,
                () -> recoverer.accept(source, new IllegalStateException("processing failed")));
    }

    @Test
    void transientFailuresRetryBeforeDurableQuarantine() {
        when(template.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(sendResult()));
        var handler = config.ingressErrorHandler(recoverer, properties, metrics());
        var consumer = mock(Consumer.class);
        var container = mock(MessageListenerContainer.class);
        when(container.isRunning()).thenReturn(true);
        var failure = new IllegalStateException("storage unavailable");

        assertFalse(handler.handleOne(failure, source, consumer, container));
        assertFalse(handler.handleOne(failure, source, consumer, container));
        verify(template, never()).send(any(ProducerRecord.class));
        assertTrue(handler.handleOne(failure, source, consumer, container));
        verify(template, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    void idempotencyConflictIsQuarantinedWithoutRedelivery() {
        when(template.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(sendResult()));
        var handler = config.ingressErrorHandler(recoverer, properties, metrics());

        assertTrue(handler.handleOne(new IdempotencyConflictException("delivery-1"), source,
                mock(Consumer.class), mock(MessageListenerContainer.class)));
        verify(template, times(1)).send(any(ProducerRecord.class));
    }

    private SendResult<Object, Object> sendResult() {
        var output = new ProducerRecord<Object, Object>(DeliveryTopics.DELIVERY_REQUESTED_DLT, 1, source.key(), source.value());
        return new SendResult<>(output, new RecordMetadata(new TopicPartition(output.topic(), 1), 0, 0, 0, 0, 0));
    }
    private static event.common.metrics.DeliveryMetrics metrics() {
        return new event.common.metrics.DeliveryMetrics(new io.micrometer.core.instrument.simple.SimpleMeterRegistry());
    }

}
