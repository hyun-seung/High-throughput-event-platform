package messaging.result;

import messaging.common.messages.MessageCategory;
import messaging.common.messages.MessageOriginCodec;
import messaging.common.messages.MessageSubmission;
import messaging.common.messages.PrimaryStageDecision;
import messaging.common.messages.SecondarySendCommand;
import messaging.common.messages.TcpSendResult;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class SecondaryResultServiceTest {
    private static final String ID = "c".repeat(32);
    private static final Instant NOW = Instant.parse("2026-10-07T12:00:00Z");
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final DynamoDbClient db = mock(DynamoDbClient.class);
    private final SecondaryResultService service = new SecondaryResultService(db, mapper);
    private final MessageSubmission submission = new MessageSubmission(ID, 42L, "customer-1",
            "01012345678", MessageCategory.GENERAL, Map.of("text", "first"),
            Map.of("text", "second"), NOW.minusSeconds(30));
    private final PrimaryStageDecision primary = new PrimaryStageDecision("primary-result", ID,
            PrimaryStageDecision.Kind.FAILURE, "HTTP_RESPONSE", 66999, null,
            null, null, true, NOW);
    private final SecondarySendCommand command = SecondarySendCommand.from(primary, submission);
    private final TcpSendResult tcp = new TcpSendResult(TcpSendResult.id(ID, command.attemptId()),
            ID, command.attemptId(), "TCP_RESPONSE", TcpSendResult.Status.SUCCESS, null, "RECEIVED", NOW);
    private final MessageResultInboxStore.Item item = new MessageResultInboxStore.Item(ID,
            tcp.resultId(), "TCP_RESPONSE", mapper.writeValueAsString(tcp), NOW);

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void matchingTcpObservationClosesSecondaryAndCreatesFinalHandoffAtomically() {
        var origin = new HashMap<>(MessageOriginCodec.encode(submission, mapper));
        origin.put("status", AttributeValue.fromS("SECONDARY_PENDING"));
        origin.put("primary_decision_id", AttributeValue.fromS(primary.decisionId()));
        origin.put("primary_decision", AttributeValue.fromS(mapper.writeValueAsString(primary)));
        var attempt = Map.of("command", AttributeValue.fromS(mapper.writeValueAsString(command)),
                "tcp_observation", AttributeValue.fromS(mapper.writeValueAsString(tcp)));
        when(db.getItem(any(GetItemRequest.class))).thenAnswer(call -> {
            var request = call.getArgument(0, GetItemRequest.class);
            return GetItemResponse.builder().item("ORIGIN".equals(request.tableName()) ? origin : attempt).build();
        });

        assertEquals(SecondaryResultService.Outcome.STORED, service.process(item));

        var capture = org.mockito.ArgumentCaptor.forClass(Consumer.class);
        verify(db).transactWriteItems(capture.capture());
        var builder = TransactWriteItemsRequest.builder();
        ((Consumer<TransactWriteItemsRequest.Builder>) capture.getValue()).accept(builder);
        var writes = builder.build().transactItems();
        assertEquals("SECONDARY_SUCCEEDED", writes.get(0).update().expressionAttributeValues().get(":final").s());
        assertEquals("SECONDARY_DECISION#" + tcp.resultId(), writes.get(1).put().item().get("sk").s());
    }

    @Test
    void unknownAttemptIsNotFinalized() {
        var origin = new HashMap<>(MessageOriginCodec.encode(submission, mapper));
        origin.put("status", AttributeValue.fromS("SECONDARY_PENDING"));
        origin.put("primary_decision", AttributeValue.fromS(mapper.writeValueAsString(primary)));
        when(db.getItem(any(GetItemRequest.class))).thenReturn(GetItemResponse.builder().item(origin).build());
        var other = new TcpSendResult(TcpSendResult.id(ID, "other"), ID, "other", "TCP_RESPONSE",
                TcpSendResult.Status.SUCCESS, null, "RECEIVED", NOW);
        var wrong = new MessageResultInboxStore.Item(ID, other.resultId(), "TCP_RESPONSE",
                mapper.writeValueAsString(other), NOW);

        assertThrows(IllegalStateException.class, () -> service.process(wrong));
        verify(db, never()).transactWriteItems(any(Consumer.class));
    }
}
