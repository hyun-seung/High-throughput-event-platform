package messaging.result;

import messaging.common.messages.MessageTopics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Objects;

/** A Kafka offset is completed only after every item is durably captured. */
@Component
public class MessageResultConsumer {
    private final MessageResultRecordCodec codec;
    private final MessageResultInboxStore inbox;
    private final HttpFailureFollowupService http;

    public MessageResultConsumer(MessageResultRecordCodec codec, MessageResultInboxStore inbox,
                                 HttpFailureFollowupService http) {
        this.codec = Objects.requireNonNull(codec);
        this.inbox = Objects.requireNonNull(inbox);
        this.http = Objects.requireNonNull(http);
    }

    @KafkaListener(topics = MessageTopics.MSG_RESULT, groupId = "messaging-result-manager")
    public void receive(ConsumerRecord<String, String> record) {
        MessageResultInput input = codec.decode(record.key(), record.value());
        var items = inbox.capture(input);
        if (input instanceof MessageResultInput.Http failure) {
            var outcome = http.process(failure.result());
            if (outcome instanceof HttpFailureFollowupService.FollowupStored
                    || outcome instanceof HttpFailureFollowupService.Ignored) {
                inbox.processed(items.getFirst());
            }
        }
        // Other results remain indexed for the decision scheduler; durable inbox capture
        // makes the Kafka offset safe to commit before that later processing completes.
    }
}
