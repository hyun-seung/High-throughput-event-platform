package messaging.common.messages;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MessageWebhookBatchTest {
    private final MessageWebhookResult success = new MessageWebhookResult("a".repeat(32), "success", null);
    private final Instant now = Instant.parse("2026-10-07T00:00:00Z");

    @Test
    void acceptsOneToOneHundredDistinctResultsAndCopiesInput() {
        var source = new ArrayList<>(List.of(success));
        var batch = new MessageWebhookBatch("trace-1", "WEBHOOK", HttpCarrier.SKT, now, source);
        source.clear();
        assertEquals(List.of(success), batch.results());
        assertThrows(UnsupportedOperationException.class, () -> batch.results().clear());

        var hundred = new ArrayList<MessageWebhookResult>();
        for (int i = 0; i < 100; i++) {
            hundred.add(new MessageWebhookResult("id-" + i, "success", null));
        }
        assertEquals(100, new MessageWebhookBatch("trace-2", "WEBHOOK", HttpCarrier.KT, now, hundred).results().size());
        hundred.add(new MessageWebhookResult("id-100", "success", null));
        assertThrows(IllegalArgumentException.class,
                () -> new MessageWebhookBatch("trace-3", "WEBHOOK", HttpCarrier.KT, now, hundred));
    }

    @Test
    void rejectsEmptyAndDuplicateBatches() {
        assertThrows(IllegalArgumentException.class,
                () -> new MessageWebhookBatch("trace", "WEBHOOK", HttpCarrier.SKT, now, List.of()));
        assertThrows(IllegalArgumentException.class,
                () -> new MessageWebhookBatch("trace", "WEBHOOK", HttpCarrier.SKT, now, List.of(success, success)));
    }

    @Test
    void statusDeterminesPresenceOfFiveDigitError() {
        assertThrows(IllegalArgumentException.class,
                () -> new MessageWebhookResult("id", "fail", null));
        assertThrows(IllegalArgumentException.class,
                () -> new MessageWebhookResult("id", "success", new MessageWebhookResult.Error(66001, "not ours")));
        assertThrows(IllegalArgumentException.class,
                () -> new MessageWebhookResult("id", "fail", new MessageWebhookResult.Error(9999, "bad")));
        assertEquals(66001, new MessageWebhookResult("id", "fail",
                new MessageWebhookResult.Error(66001, "not ours")).error().code());
    }
}
