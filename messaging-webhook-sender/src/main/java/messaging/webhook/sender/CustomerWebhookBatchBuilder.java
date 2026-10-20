package messaging.webhook.sender;

import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Objects;
import java.util.UUID;

/** Selects an ordered prefix without serializing all preceding results on every append. */
final class CustomerWebhookBatchBuilder {
    private final JsonMapper mapper;
    private final UUID batchId;
    private final long clientId;
    private final int maxBytes;
    private final boolean incrementalSize;
    private final ArrayList<CustomerWebhookBatch.Result> results = new ArrayList<>();
    private int bytes;
    private String fullBody;

    CustomerWebhookBatchBuilder(JsonMapper mapper, UUID batchId, long clientId, int maxBytes) {
        this.mapper = Objects.requireNonNull(mapper);
        this.batchId = Objects.requireNonNull(batchId);
        this.clientId = clientId;
        this.maxBytes = maxBytes;
        // Keep non-compact/root-wrapped/array-unwrapped mapper output compatible too.
        incrementalSize = !mapper.isEnabled(SerializationFeature.INDENT_OUTPUT)
                && !mapper.isEnabled(SerializationFeature.WRAP_ROOT_VALUE)
                && !mapper.isEnabled(SerializationFeature.WRITE_SINGLE_ELEM_ARRAYS_UNWRAPPED);
    }

    boolean add(CustomerWebhookBatch.Result result) {
        if (results.size() == 100) return false;
        results.add(Objects.requireNonNull(result));
        boolean fullEncoding = results.size() == 1 || !incrementalSize;
        String candidate = mapper.writeValueAsString(fullEncoding ? batch() : result);
        // The first full encoding includes every envelope field, brackets and the first result.
        // Each additional result in a compact JSON array contributes its UTF-8 bytes and a comma.
        int candidateBytes = candidate.getBytes(StandardCharsets.UTF_8).length + (fullEncoding ? 0 : bytes + 1);
        if (candidateBytes > maxBytes) {
            results.removeLast();
            return false;
        }
        bytes = candidateBytes;
        if (fullEncoding) fullBody = candidate;
        return true;
    }

    String body() {
        if (results.isEmpty()) throw new IllegalStateException("Single customer webhook exceeds batch byte limit");
        return results.size() == 1 || !incrementalSize ? fullBody : mapper.writeValueAsString(batch());
    }

    private CustomerWebhookBatch batch() {
        return new CustomerWebhookBatch(1, batchId, clientId, results);
    }
}
