package messaging.pre.send;

import messaging.common.messages.MessageSubmission;
import messaging.common.messages.MessageTopics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.concurrent.ExecutionException;

@Component
public class PreSendReceivedConsumer {
    private final PreSendDecisionStore decisions;
    private final KafkaTemplate<String, Object> kafka;

    public PreSendReceivedConsumer(PreSendDecisionStore decisions, KafkaTemplate<String, Object> kafka) {
        this.decisions = Objects.requireNonNull(decisions);
        this.kafka = Objects.requireNonNull(kafka);
    }

    @KafkaListener(topics = MessageTopics.RECEIVED, groupId = "messaging-pre-send-manager")
    public void receive(ConsumerRecord<String, MessageSubmission> record) throws ExecutionException, InterruptedException {
        MessageSubmission admission = Objects.requireNonNull(record.value(), "Missing message admission");
        if (!admission.clientMsgId().equals(record.key())) {
            throw new IllegalArgumentException("Kafka admission key does not match clientMsgId");
        }
        var selected = decisions.prepareOrLoad(admission);
        if (selected.isEmpty()) return;
        PreSendDispatch dispatch = selected.get();
        String topic = dispatch.command() != null
                ? MessageTopics.httpSend(dispatch.command().carrier()) : MessageTopics.MSG_RESULT;
        Object value = dispatch.command() != null ? dispatch.command() : dispatch.failure();
        try {
            kafka.send(topic, admission.clientMsgId(), value).get();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw interrupted;
        }
    }
}
