package event.verification;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PocItemReaderTest {
    @Test
    void readsOriginBeforeTheActualExecutionAndPreservesProjectedShape() throws Exception {
        var db = mock(DynamoDbClient.class);
        String request = "request-id", execution = "execution-id";
        var origin = item("DELIVERY#" + request, "META", "delivery_id", execution);
        var step = item("DELIVERY#" + execution, "ATTEMPT#" + attempt(execution), "status", "ACCEPTED");
        var requests = new ArrayList<BatchGetItemRequest>();
        when(db.batchGetItem(any(BatchGetItemRequest.class))).thenAnswer(call -> {
            BatchGetItemRequest batch = call.getArgument(0);
            requests.add(batch);
            return BatchGetItemResponse.builder().responses(requests.size() == 1 ? Map.of("ORIGIN", List.of(origin))
                    : Map.of("STEP", List.of(step))).build();
        });
        var items = PocItemReader.read(db, List.of(request), ignored -> {});
        assertEquals(2, items.size());
        assertEquals("ACCEPTED", items.get(1).get("status").get("S"));
        assertEquals("DELIVERY#" + execution, requests.get(1).requestItems().get("STEP").keys().getFirst().get("pk").s());
        assertEquals("ATTEMPT#" + attempt(execution), requests.get(1).requestItems().get("STEP").keys().getFirst().get("sk").s());
        assertTrue(requests.getFirst().requestItems().get("ORIGIN").consistentRead());
        assertTrue(requests.get(1).requestItems().get("STEP").consistentRead());
        assertEquals("status", requests.getFirst().requestItems().get("ORIGIN").expressionAttributeNames().get("#s"));
    }

    @Test
    void retriesOnlyUnprocessedKeysAndFailsAfterSixAttempts() throws Exception {
        var db = mock(DynamoDbClient.class);
        var seen = new ArrayList<BatchGetItemRequest>();
        var waits = new ArrayList<Long>();
        when(db.batchGetItem(any(BatchGetItemRequest.class))).thenAnswer(call -> {
            BatchGetItemRequest request = call.getArgument(0);
            seen.add(request);
            if (seen.size() == 1) return BatchGetItemResponse.builder().unprocessedKeys(request.requestItems()).build();
            return BatchGetItemResponse.builder().responses(Map.of("ORIGIN", List.of(
                    item("DELIVERY#request", "META", "status", "ACCEPTED")))).build();
        });
        PocItemReader.read(db, List.of("request"), waits::add);
        assertEquals(List.of(100L), waits);
        assertEquals(seen.getFirst().requestItems(), seen.get(1).requestItems());

        var stuck = mock(DynamoDbClient.class);
        when(stuck.batchGetItem(any(BatchGetItemRequest.class))).thenAnswer(call -> {
            BatchGetItemRequest request = call.getArgument(0);
            return BatchGetItemResponse.builder().unprocessedKeys(request.requestItems()).build();
        });
        var delay = new ArrayList<Long>();
        assertThrows(IllegalStateException.class, () -> PocItemReader.read(stuck, List.of("request"), delay::add));
        assertEquals(List.of(100L, 200L, 400L, 800L, 1600L), delay);
    }

    @Test
    void splitsBothTableReadsAtTheBatchLimit() throws Exception {
        var db = mock(DynamoDbClient.class);
        var requests = new ArrayList<BatchGetItemRequest>();
        when(db.batchGetItem(any(BatchGetItemRequest.class))).thenAnswer(call -> {
            requests.add(call.getArgument(0));
            return BatchGetItemResponse.builder().build();
        });
        var deliveries = new ArrayList<String>();
        for (int i = 0; i < 101; i++) deliveries.add("request-" + i);
        PocItemReader.read(db, deliveries, ignored -> {});
        assertEquals(4, requests.size());
        assertEquals(100, requests.getFirst().requestItems().get("ORIGIN").keys().size());
        assertEquals(1, requests.get(1).requestItems().get("ORIGIN").keys().size());
        assertEquals(100, requests.get(2).requestItems().get("STEP").keys().size());
        assertEquals(1, requests.get(3).requestItems().get("STEP").keys().size());
    }

    private static Map<String, AttributeValue> item(String pk, String sk, String name, String value) {
        return Map.of("pk", AttributeValue.fromS(pk), "sk", AttributeValue.fromS(sk), name, AttributeValue.fromS(value));
    }

    private static String attempt(String execution) {
        return UUID.nameUUIDFromBytes(("attempt:" + execution + ":mock-provider:1:1")
                .getBytes(java.nio.charset.StandardCharsets.UTF_8)).toString();
    }
}
