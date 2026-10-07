package messaging.result;

import messaging.common.messages.FollowupHttpCommand;
import messaging.common.messages.FollowupDispatchIndex;
import messaging.common.messages.HttpCarrier;
import messaging.common.messages.HttpProviderRequest;
import messaging.common.messages.HttpSendCommand;
import messaging.common.messages.MessageOriginCodec;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CancellationReason;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.Map;
import java.util.HashMap;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class FollowupHttpCommandStoreTest {
    private final DynamoDbClient db = mock(DynamoDbClient.class);
    private final FollowupHttpCommandStore store = new FollowupHttpCommandStore(db, JsonMapper.builder().build());
    private final Instant now = Instant.parse("2026-10-07T00:00:00Z");
    private final String clientMsgId = "a".repeat(32);
    private final FollowupHttpCommand followup = new FollowupHttpCommand("result-1",
            new HttpSendCommand("attempt-kt", HttpCarrier.KT, 1, now.plusSeconds(3600),
                    new HttpProviderRequest(clientMsgId, 42L, "GENERAL", "01012345678",
                            Map.of("text", "hello"), now)), now.plusSeconds(60));

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void freezesDecisionAndCommandInOneTransaction() {
        assertEquals(followup, store.freeze(clientMsgId, null, followup));

        var capture = org.mockito.ArgumentCaptor.forClass(Consumer.class);
        verify(db).transactWriteItems(capture.capture());
        var builder = TransactWriteItemsRequest.builder();
        ((Consumer<TransactWriteItemsRequest.Builder>) capture.getValue()).accept(builder);
        var writes = builder.build().transactItems();
        assertEquals(2, writes.size());
        assertTrue(writes.get(0).update().conditionExpression().contains("attribute_not_exists(#decision)"));
        assertEquals("result-1", writes.get(0).update().expressionAttributeValues().get(":next").s());
        assertEquals(JsonMapper.builder().build().writeValueAsString(followup.command()),
                writes.get(0).update().expressionAttributeValues().get(":command").s());
        assertEquals("result-1", writes.get(1).put().item().get("decision_id").s());
        assertEquals(followup.notBefore().toEpochMilli(),
                Long.parseLong(writes.get(1).put().item().get("not_before_ms").n()));
        assertEquals(FollowupDispatchIndex.bucket(clientMsgId),
                writes.get(1).put().item().get(FollowupDispatchIndex.BUCKET).s());
        assertEquals(followup.notBefore().toEpochMilli(),
                Long.parseLong(writes.get(1).put().item().get(FollowupDispatchIndex.DUE).n()));
    }

    @Test
    void cannotReuseTheSameDecisionAsItsPredecessor() {
        assertThrows(IllegalArgumentException.class,
                () -> store.freeze(clientMsgId, followup.decisionId(), followup));
        verifyNoInteractions(db);
    }

    @Test
    void lostTransactionAcknowledgementLoadsTheSameFrozenCommand() {
        var origin = new HashMap<>(MessageOriginCodec.key(clientMsgId));
        origin.put(FollowupHttpCommand.CURRENT_DECISION, AttributeValue.fromS(followup.decisionId()));
        origin.put(FollowupHttpCommand.CURRENT_COMMAND,
                AttributeValue.fromS(JsonMapper.builder().build().writeValueAsString(followup.command())));
        var command = new HashMap<>(FollowupHttpCommand.key(followup.command()));
        command.put("authorization", AttributeValue.fromS(JsonMapper.builder().build().writeValueAsString(followup)));
        when(db.transactWriteItems(any(Consumer.class))).thenThrow(TransactionCanceledException.builder()
                .cancellationReasons(CancellationReason.builder().code("ConditionalCheckFailed").build()).build());
        when(db.getItem(any(software.amazon.awssdk.services.dynamodb.model.GetItemRequest.class)))
                .thenAnswer(call -> GetItemResponse.builder().item(call.getArgument(0,
                        software.amazon.awssdk.services.dynamodb.model.GetItemRequest.class)
                        .tableName().equals(messaging.common.dynamodb.DynamoDbTableNames.ORIGIN)
                        ? origin : command).build());

        assertEquals(followup, store.freeze(clientMsgId, null, followup));
    }
}
