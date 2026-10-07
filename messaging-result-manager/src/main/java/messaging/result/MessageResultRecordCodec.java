package messaging.result;

import messaging.common.messages.CarrierHttpResult;
import messaging.common.messages.MessageWebhookBatch;
import messaging.common.messages.PreSendFailure;
import messaging.common.messages.TcpSendResult;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.util.Objects;

/** MSG_RESULT carries several JSON records without Kafka type headers. */
public final class MessageResultRecordCodec {
    private final JsonMapper mapper;

    public MessageResultRecordCodec(JsonMapper mapper) {
        this.mapper = Objects.requireNonNull(mapper);
    }

    public MessageResultInput decode(String key, String json) {
        if (key == null || key.isBlank() || json == null || json.isBlank()) {
            throw new IllegalArgumentException("MSG_RESULT key and JSON are required");
        }
        JsonNode root = mapper.readTree(json);
        if (root == null || !root.isObject()) throw new IllegalArgumentException("MSG_RESULT must be a JSON object");
        JsonNode source = root.get("source");
        if (source == null || !source.isTextual()) throw new IllegalArgumentException("MSG_RESULT source is required");
        return switch (source.asText()) {
            case "WEBHOOK" -> {
                MessageWebhookBatch batch = mapper.readValue(json, MessageWebhookBatch.class);
                requireKey(key, batch.traceId());
                yield new MessageResultInput.Webhook(batch);
            }
            case "HTTP_RESPONSE", "HTTP_TIMEOUT" -> {
                CarrierHttpResult result = mapper.readValue(json, CarrierHttpResult.class);
                requireKey(key, result.clientMsgId());
                if (result.status() == CarrierHttpResult.Status.ACCEPTED) {
                    throw new IllegalArgumentException("HTTP 200 must not be published to MSG_RESULT");
                }
                yield new MessageResultInput.Http(result);
            }
            case "PRE_SEND" -> {
                PreSendFailure failure = mapper.readValue(json, PreSendFailure.class);
                requireKey(key, failure.clientMsgId());
                yield new MessageResultInput.PreSend(failure);
            }
            case "TCP_RESPONSE" -> {
                TcpSendResult result = mapper.readValue(json, TcpSendResult.class);
                requireKey(key, result.clientMsgId());
                yield new MessageResultInput.Tcp(result);
            }
            default -> throw new IllegalArgumentException("Unsupported MSG_RESULT source: " + source.asText());
        };
    }

    private static void requireKey(String actual, String expected) {
        if (!actual.equals(expected)) throw new IllegalArgumentException("MSG_RESULT key does not match payload");
    }
}
