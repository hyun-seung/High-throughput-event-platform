package messaging.result;

import messaging.common.messages.MessageTopics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.time.Clock;

/** Processes captured inputs and confirms downstream Kafka acks before completing the offset. */
@Component
public class MessageResultConsumer {
    private final MessageResultRecordCodec codec;
    private final MessageResultInboxStore inbox;
    private final PendingResultDispatcher dispatcher;
    private final Clock clock;

    public MessageResultConsumer(MessageResultRecordCodec codec, MessageResultInboxStore inbox,
                                 PendingResultDispatcher dispatcher, Clock clock) {
        this.codec = Objects.requireNonNull(codec);
        this.inbox = Objects.requireNonNull(inbox);
        this.dispatcher = Objects.requireNonNull(dispatcher);
        this.clock = Objects.requireNonNull(clock);
    }

    @KafkaListener(topics = MessageTopics.MSG_RESULT, groupId = "messaging-result-manager")
    public void receive(ConsumerRecord<String, String> record) {
        MessageResultInput input = codec.decode(record.key(), record.value());
        var items = inbox.capture(input);
        for (var item : items) dispatcher.process(item, clock.instant());
        // Inputs awaiting another observation stay in the durable inbox. Kafka errors
        // propagate to the listener's retry handler instead of committing this offset.
    }
}
