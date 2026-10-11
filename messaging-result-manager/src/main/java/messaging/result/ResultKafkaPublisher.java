package messaging.result;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import messaging.common.messages.CustomerWebhookSendCommand;
import messaging.common.messages.FinalizedMessageResult;
import messaging.common.messages.MessageOriginCodec;
import messaging.common.messages.MessageTopics;
import messaging.common.messages.PrimaryExpiryIndex;
import messaging.common.messages.PrimaryStageDecision;
import messaging.common.messages.SecondarySendCommand;
import messaging.common.messages.SecondaryStageDecision;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import tools.jackson.databind.json.JsonMapper;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static messaging.common.dynamodb.DynamoDbTableNames.ORIGIN;

/** Calls Kafka in the result-processing thread; a failed ack keeps the input retryable. */
@Component
public class ResultKafkaPublisher {
    private static final String PRIMARY_ACK = "primary_kafka_ack_decision_id";
    private final DynamoDbClient db;
    private final JsonMapper mapper;
    private final KafkaTemplate<String, Object> kafka;
    private final long timeoutMs;
    private final MeterRegistry registry;

    public ResultKafkaPublisher(DynamoDbClient db, JsonMapper mapper, KafkaTemplate<String, Object> kafka,
                                @Value("${messaging.result.kafka.publish-timeout-ms:10000}") long timeoutMs,
                                MeterRegistry registry) {
        this.db = Objects.requireNonNull(db);
        this.mapper = Objects.requireNonNull(mapper);
        this.kafka = Objects.requireNonNull(kafka);
        if (timeoutMs < 1) throw new IllegalArgumentException("Invalid Kafka ack timeout");
        this.timeoutMs = timeoutMs;
        this.registry = Objects.requireNonNull(registry);
    }

    public void publish(Map<String, AttributeValue> origin) {
        if (origin == null || !origin.containsKey("primary_decision")) return;
        var primary = mapper.readValue(origin.get("primary_decision").s(), PrimaryStageDecision.class);
        var secondary = origin.containsKey("secondary_decision")
                ? mapper.readValue(origin.get("secondary_decision").s(), SecondaryStageDecision.class) : null;
        boolean tcp = primary.secondaryRequired() && secondary == null;
        String id = tcp || secondary == null ? primary.decisionId() : secondary.decisionId();
        String ackField = tcp ? PRIMARY_ACK : MessageOriginCodec.FINAL_KAFKA_ACK_DECISION_ID;
        if (id.equals(origin.getOrDefault(ackField, s("")).s())) return;
        var submission = MessageOriginCodec.decode(origin, mapper);
        if (!primary.clientMsgId().equals(submission.clientMsgId())) {
            throw new IllegalStateException("Frozen result belongs to another message");
        }
        if (tcp) {
            send(MessageTopics.TCP_SEND, primary.clientMsgId(), SecondarySendCommand.from(primary, submission));
        } else {
            var finalized = secondary == null ? new FinalizedMessageResult(primary, submission)
                    : new FinalizedMessageResult(primary, secondary, submission);
            send(MessageTopics.MSG_RESULT_FINALIZED, primary.clientMsgId(), finalized);
            send(MessageTopics.WEBHOOK_SEND, primary.clientMsgId(), CustomerWebhookSendCommand.from(finalized));
        }
        // This is an ack receipt on business state, not a queued outgoing event.
        // Cleanup can proceed only after BOTH final-result and webhook Kafka acks.
        String decisionField = secondary == null ? "primary_decision_id" : "secondary_decision_id";
        try {
            db.updateItem(UpdateItemRequest.builder().tableName(ORIGIN)
                    .key(MessageOriginCodec.key(primary.clientMsgId()))
                    .conditionExpression("#decision = :id AND attribute_exists(pk)")
                    .updateExpression("SET #ack = :id REMOVE #expiryBucket, #expiryDue")
                    .expressionAttributeNames(Map.of("#decision", decisionField, "#ack", ackField,
                            "#expiryBucket", PrimaryExpiryIndex.BUCKET, "#expiryDue", PrimaryExpiryIndex.DUE))
                    .expressionAttributeValues(Map.of(":id", s(id))).build());
        } catch (ConditionalCheckFailedException changed) {
            var current = db.getItem(GetItemRequest.builder().tableName(ORIGIN)
                    .key(MessageOriginCodec.key(primary.clientMsgId())).consistentRead(true).build()).item();
            if (!current.isEmpty() && !id.equals(current.getOrDefault(ackField, s("")).s())) throw changed;
        }
    }

    private void send(String topic, String clientMsgId, Object value) {
        var timing = Timer.start(registry);
        String outcome = "error";
        try {
            kafka.send(topic, clientMsgId, value).get(timeoutMs, TimeUnit.MILLISECONDS);
            outcome = "success";
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Result Kafka ack interrupted", interrupted);
        } catch (ExecutionException | TimeoutException failure) {
            throw new IllegalStateException("Result Kafka ack failed: " + topic, failure);
        } finally {
            timing.stop(registry.timer("messaging.result.kafka.publish", "topic", topic, "result", outcome));
        }
    }

    private static AttributeValue s(String value) { return AttributeValue.fromS(value); }
}
