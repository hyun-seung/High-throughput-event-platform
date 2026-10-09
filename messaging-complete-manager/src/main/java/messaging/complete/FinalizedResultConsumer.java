package messaging.complete;

import messaging.common.messages.FinalizedMessageResult;
import messaging.common.messages.MessageTopics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.util.Objects;
import java.util.ArrayList;
import java.util.List;

/** Acknowledges Kafka only after the SQL history and optional CDR transaction commits. */
@Component
@ConditionalOnProperty(prefix = "messaging.complete.consumer", name = "enabled", havingValue = "true")
public class FinalizedResultConsumer {
    private final FinalizedHistoryStore history;
    private final JsonMapper mapper;

    public FinalizedResultConsumer(FinalizedHistoryStore history, JsonMapper mapper) {
        this.history = Objects.requireNonNull(history);
        this.mapper = Objects.requireNonNull(mapper);
    }

    @KafkaListener(topics = MessageTopics.MSG_RESULT_FINALIZED, groupId = "messaging-complete-manager")
    public void receive(List<ConsumerRecord<String, String>> records) {
        var batch = new ArrayList<FinalizedMessageResult>(records.size());
        for (var record : records) {
            if (record.key() == null || record.value() == null) {
                throw new IllegalArgumentException("Finalized result requires key and value");
            }
            var finalized = mapper.readValue(record.value(), FinalizedMessageResult.class);
            if (!record.key().equals(finalized.decision().clientMsgId())) {
                throw new IllegalArgumentException("Finalized Kafka key differs from clientMsgId");
            }
            batch.add(finalized);
        }
        history.storeBatch(batch);
    }
}
