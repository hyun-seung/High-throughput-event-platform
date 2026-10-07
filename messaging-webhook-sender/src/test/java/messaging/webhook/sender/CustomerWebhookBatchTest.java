package messaging.webhook.sender;

import messaging.common.messages.CustomerWebhookSendCommand;
import messaging.common.messages.FinalizedMessageResult;
import messaging.common.messages.MessageCategory;
import messaging.common.messages.MessageSubmission;
import messaging.common.messages.PrimaryStageDecision;
import messaging.common.messages.SecondaryStageDecision;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CustomerWebhookBatchTest {
    @Test
    void reportsSecondarySuccessRatherThanPreviousPrimaryFailure() {
        String id = "d".repeat(32);
        var now = Instant.parse("2026-10-07T12:00:00Z");
        var primary = new PrimaryStageDecision("primary-failure", id, PrimaryStageDecision.Kind.FAILURE,
                "HTTP_RESPONSE", 66999, null, null, null, true, now);
        var secondary = new SecondaryStageDecision("tcp-success", id, "tcp-attempt",
                SecondaryStageDecision.Kind.SUCCESS, null, "RECEIVED", now.plusSeconds(1));
        var submission = new MessageSubmission(id, 42L, "customer-1", "01012345678",
                MessageCategory.GENERAL, Map.of("text", "first"), Map.of("text", "second"), now);

        var result = CustomerWebhookBatch.Result.from(CustomerWebhookSendCommand.from(
                new FinalizedMessageResult(primary, secondary, submission)));

        assertEquals("success", result.status());
        assertNull(result.errorCode());
        assertEquals(secondary.decidedAt(), result.decidedAt());
    }
}
