package messaging.result;

import messaging.common.messages.FollowupDispatchIndex;
import messaging.common.messages.FollowupHttpCommand;
import messaging.common.messages.HttpCarrier;
import messaging.common.messages.HttpProviderRequest;
import messaging.common.messages.HttpSendCommand;
import messaging.common.messages.MessageOriginCodec;
import messaging.common.messages.MessageTopics;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class FollowupHttpDispatcherTest {
    private final Instant now = Instant.parse("2026-10-07T00:01:00Z");
    private final String clientMsgId = "a".repeat(32);
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final DynamoDbClient db = mock(DynamoDbClient.class);
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, HttpSendCommand> kafka = mock(KafkaTemplate.class);
    private final FollowupHttpDispatcher dispatcher = new FollowupHttpDispatcher(db, mapper, kafka,
            Clock.fixed(now, ZoneOffset.UTC), 100);
    private final FollowupHttpCommand followup = new FollowupHttpCommand("result-1",
            new HttpSendCommand("attempt-kt", HttpCarrier.KT, 1, now.plusSeconds(3600),
                    new HttpProviderRequest(clientMsgId, 42L, "GENERAL", "01012345678",
                            Map.of("text", "hello"), now.minusSeconds(60))), now);

    @Test
    void dueCommandPublishesThenRemovesTheDueIndex() throws Exception {
        var step = step();
        var origin = origin();
        reads(step, origin);
        when(kafka.send(anyString(), anyString(), any(HttpSendCommand.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        dispatcher.dispatch(FollowupHttpCommand.key(followup.command()), now.toEpochMilli());

        verify(kafka).send(MessageTopics.KT_HTTP_SEND, clientMsgId, followup.command());
        var capture = org.mockito.ArgumentCaptor.forClass(UpdateItemRequest.class);
        verify(db).updateItem(capture.capture());
        assertTrue(capture.getValue().updateExpression().contains("REMOVE #bucket, #due"));
        assertEquals(step.get("authorization"),
                capture.getValue().expressionAttributeValues().get(":authorization"));
    }

    @Test
    void unconfirmedKafkaSendKeepsTheDueIndexForReplay() {
        reads(step(), origin());
        when(kafka.send(anyString(), anyString(), any(HttpSendCommand.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("Kafka unavailable")));

        assertThrows(ExecutionException.class,
                () -> dispatcher.dispatch(FollowupHttpCommand.key(followup.command()), now.toEpochMilli()));
        verify(db, never()).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void staleDecisionIsRemovedWithoutPublication() throws Exception {
        var origin = origin();
        origin.put(FollowupHttpCommand.CURRENT_DECISION, AttributeValue.fromS("result-2"));
        reads(step(), origin);

        dispatcher.dispatch(FollowupHttpCommand.key(followup.command()), now.toEpochMilli());

        verifyNoInteractions(kafka);
        verify(db).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void commandBeforeDueTimeIsNotPublished() throws Exception {
        reads(step(), origin());
        dispatcher.dispatch(FollowupHttpCommand.key(followup.command()), now.minusSeconds(1).toEpochMilli());
        verifyNoInteractions(kafka);
        verify(db, never()).updateItem(any(UpdateItemRequest.class));
    }

    @Test
    void expiredCommandRemainsVisibleForFailureResolution() {
        reads(step(), origin());

        assertThrows(IllegalStateException.class, () -> dispatcher.dispatch(
                FollowupHttpCommand.key(followup.command()),
                followup.command().deadlineAt().toEpochMilli()));
        verifyNoInteractions(kafka);
        verify(db, never()).updateItem(any(UpdateItemRequest.class));
    }

    private Map<String, AttributeValue> step() {
        var item = new HashMap<>(FollowupHttpCommand.key(followup.command()));
        item.put("authorization", AttributeValue.fromS(mapper.writeValueAsString(followup)));
        item.put("decision_id", AttributeValue.fromS(followup.decisionId()));
        FollowupDispatchIndex.add(item, clientMsgId, followup.notBefore().toEpochMilli());
        return item;
    }

    private Map<String, AttributeValue> origin() {
        var item = new HashMap<>(MessageOriginCodec.key(clientMsgId));
        item.put("delivery_id", AttributeValue.fromS(clientMsgId));
        item.put("status", AttributeValue.fromS(MessageOriginCodec.STATUS_RECEIVED));
        item.put(FollowupHttpCommand.CURRENT_DECISION, AttributeValue.fromS(followup.decisionId()));
        return item;
    }

    private void reads(Map<String, AttributeValue> step, Map<String, AttributeValue> origin) {
        when(db.getItem(any(GetItemRequest.class))).thenAnswer(call -> GetItemResponse.builder()
                .item(call.getArgument(0, GetItemRequest.class).tableName().equals(
                        messaging.common.dynamodb.DynamoDbTableNames.STEP) ? step : origin).build());
    }
}
