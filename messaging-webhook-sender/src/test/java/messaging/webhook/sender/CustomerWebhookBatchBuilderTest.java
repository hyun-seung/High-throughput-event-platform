package messaging.webhook.sender;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class CustomerWebhookBatchBuilderTest {
    private static final UUID BATCH_ID = UUID.fromString("00000000-0000-0000-0000-000000000123");
    private final JsonMapper mapper = JsonMapper.builder().build();

    @ParameterizedTest
    @ValueSource(ints = {1, 100})
    void preservesTheExactBodyAndOrderForSingleAndFullBatches(int count) {
        var results = results(count);
        var builder = new CustomerWebhookBatchBuilder(mapper, BATCH_ID, 42, 1_048_576);
        results.forEach(result -> assertTrue(builder.add(result)));

        String expected = encode(mapper, results);
        assertEquals(expected, builder.body());
        assertArrayEquals(expected.getBytes(StandardCharsets.UTF_8), builder.body().getBytes(StandardCharsets.UTF_8));
        assertEquals(results, mapper.readValue(builder.body(), CustomerWebhookBatch.class).results());
    }

    @Test
    void selectsTheSamePrefixAtExactUtf8Boundaries() {
        var results = results(100);
        for (int count : new int[] {1, 2, 7, 99, 100}) {
            String body = encode(mapper, results.subList(0, count));
            int exactBytes = body.getBytes(StandardCharsets.UTF_8).length;
            assertTrue(exactBytes > body.length(), "Use multibyte content so a character count would be incorrect");
            for (int delta : new int[] {-1, 0, 1}) {
                int maxBytes = exactBytes + delta;
                var builder = new CustomerWebhookBatchBuilder(mapper, BATCH_ID, 42, maxBytes);
                int accepted = 0;
                for (var result : results) {
                    if (!builder.add(result)) break;
                    accepted++;
                }
                String expected = legacyPrefix(mapper, results, maxBytes);
                if (expected == null) assertThrows(IllegalStateException.class, builder::body);
                else {
                    assertEquals(expected, builder.body(), "Byte limit " + maxBytes);
                    assertTrue(builder.body().getBytes(StandardCharsets.UTF_8).length <= maxBytes);
                }
                assertEquals(delta < 0 ? count - 1 : count, accepted);
            }
        }
    }

    @Test
    void rejectsThe101stResultWithoutChangingTheBody() {
        var results = results(101);
        var builder = new CustomerWebhookBatchBuilder(mapper, BATCH_ID, 42, 1_048_576);
        results.subList(0, 100).forEach(result -> assertTrue(builder.add(result)));
        String body = builder.body();
        assertFalse(builder.add(results.get(100)));
        assertEquals(body, builder.body());
    }

    @Test
    void oversizedFirstResultCannotProduceAnEmptyBatch() {
        var result = results(1).getFirst();
        int bytes = encode(mapper, List.of(result)).getBytes(StandardCharsets.UTF_8).length;
        var builder = new CustomerWebhookBatchBuilder(mapper, BATCH_ID, 42, bytes - 1);
        assertFalse(builder.add(result));
        assertThrows(IllegalStateException.class, builder::body);
    }

    @ParameterizedTest
    @EnumSource(value = SerializationFeature.class,
            names = {"INDENT_OUTPUT", "WRAP_ROOT_VALUE", "WRITE_SINGLE_ELEM_ARRAYS_UNWRAPPED"})
    void preservesConfiguredJsonLayoutsAndTheirByteLimits(SerializationFeature feature) {
        var configured = JsonMapper.builder().enable(feature).build();
        var results = results(3);
        int maxBytes = encode(configured, results.subList(0, 2)).getBytes(StandardCharsets.UTF_8).length;
        var builder = new CustomerWebhookBatchBuilder(configured, BATCH_ID, 42, maxBytes);
        assertTrue(builder.add(results.get(0)));
        assertTrue(builder.add(results.get(1)));
        assertFalse(builder.add(results.get(2)));
        assertEquals(legacyPrefix(configured, results, maxBytes), builder.body());
    }

    private static List<CustomerWebhookBatch.Result> results(int count) {
        return IntStream.range(0, count).mapToObj(index -> new CustomerWebhookBatch.Result(
                new UUID(0, index), "%032d".formatted(index), "고객😀中\"\\\r\n\t-" + index,
                index % 2 == 0 ? "success" : "fail", index % 2 == 0 ? null : 66003,
                index % 2 == 0 ? null : "실패\b\f\"\\😀", Instant.parse("2026-10-20T00:00:00Z"))).toList();
    }

    private static String encode(JsonMapper mapper, List<CustomerWebhookBatch.Result> results) {
        return mapper.writeValueAsString(new CustomerWebhookBatch(1, BATCH_ID, 42, results));
    }

    /** The previous production algorithm is the compatibility oracle. */
    private static String legacyPrefix(JsonMapper mapper, List<CustomerWebhookBatch.Result> rows, int maxBytes) {
        var selected = new ArrayList<CustomerWebhookBatch.Result>();
        String body = null;
        for (var row : rows) {
            selected.add(row);
            String candidate = encode(mapper, selected);
            if (candidate.getBytes(StandardCharsets.UTF_8).length > maxBytes) break;
            body = candidate;
        }
        return body;
    }
}
