package messaging.result;

import messaging.common.messages.CarrierHttpResult;
import messaging.common.messages.HttpCarrier;
import messaging.common.messages.MessageWebhookBatch;
import messaging.common.messages.MessageWebhookResult;
import messaging.common.messages.PreSendFailure;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MessageResultRecordCodecTest {
    private static final Instant NOW = Instant.parse("2026-10-07T00:00:00Z");
    private static final String ID = "a".repeat(32);
    private final JsonMapper mapper = JsonMapper.builder().build();
    private final MessageResultRecordCodec codec = new MessageResultRecordCodec(mapper);

    @Test
    void decodesOneWebhookRecordContainingIndependentResults() {
        var batch = new MessageWebhookBatch("trace-1", "WEBHOOK", HttpCarrier.KT, NOW,
                List.of(new MessageWebhookResult(ID, "success", null),
                        new MessageWebhookResult("b".repeat(32), "fail",
                                new MessageWebhookResult.Error(66002, "tps"))));
        assertEquals(new MessageResultInput.Webhook(batch), codec.decode("trace-1", mapper.writeValueAsString(batch)));
        assertThrows(IllegalArgumentException.class,
                () -> codec.decode(ID, mapper.writeValueAsString(batch)));
    }

    @Test
    void decodesExplicitFailureAndTimeoutAsSingleMessageInputs() {
        var failure = new CarrierHttpResult("result-1", ID, "attempt-1", HttpCarrier.KT, 1,
                "HTTP_RESPONSE", CarrierHttpResult.Status.FAILED, 400, "4xx", "41001", 66001,
                "not ours", NOW);
        var timeout = new CarrierHttpResult("result-2", ID, "attempt-2", HttpCarrier.KT, 2,
                "HTTP_TIMEOUT", CarrierHttpResult.Status.TIMEOUT, null, null, null, null, null, NOW);
        assertEquals(new MessageResultInput.Http(failure), codec.decode(ID, mapper.writeValueAsString(failure)));
        assertEquals(new MessageResultInput.Http(timeout), codec.decode(ID, mapper.writeValueAsString(timeout)));
    }

    @Test
    void decodesPreSendRejectionAndRejectsAnHttpAcceptance() {
        var preSend = PreSendFailure.of(ID, PreSendFailure.Reason.CONTRACT_DISABLED, NOW);
        assertEquals(new MessageResultInput.PreSend(preSend), codec.decode(ID, mapper.writeValueAsString(preSend)));
        var accepted = new CarrierHttpResult("result-1", ID, "attempt-1", HttpCarrier.KT, 1,
                "HTTP_RESPONSE", CarrierHttpResult.Status.ACCEPTED, 200, null, null, null, null, NOW);
        assertThrows(IllegalArgumentException.class,
                () -> codec.decode(ID, mapper.writeValueAsString(accepted)));
    }

    @Test
    void rejectsUnknownSourceAndInvalidWebhookItem() {
        assertThrows(IllegalArgumentException.class,
                () -> codec.decode(ID, "{\"source\":\"TCP_RESPONSE\"}"));
        assertThrows(RuntimeException.class,
                () -> codec.decode("trace-1", "{\"traceId\":\"trace-1\",\"source\":\"WEBHOOK\","
                        + "\"carrier\":\"KT\",\"receivedAt\":\"2026-10-07T00:00:00Z\","
                        + "\"results\":[{\"clientMsgId\":\"" + ID + "\",\"status\":\"fail\"}]}"));
    }
}
