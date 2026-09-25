package external.api.simulator.customer;

import event.common.notification.CustomerResultBatch;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

/** Bounded process-local receiver for customer HTTP tests; never queried by delivery decisions. */
@RestController
@ConditionalOnProperty(name = "simulator.customer-results.enabled", havingValue = "true")
public class CustomerResultSimulator {
    public enum Mode { ACK, REJECT, FAIL_ONCE, ACK_THEN_503, DELAY_ACK }
    private record Seen(String digest, long requests) {}
    private final Map<String, Seen> batches = new HashMap<>();
    private final Set<String> results = new HashSet<>();
    private final byte[] secret;
    private final int maxResults;
    private final long delayMillis;
    private long requests;
    private final JsonMapper mapper = JsonMapper.builder().enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).build();

    public CustomerResultSimulator(@Value("${simulator.customer-results.secret:}") String secret,
            @Value("${simulator.customer-results.max-results:100000}") int maxResults,
            @Value("${simulator.customer-results.delay-millis:5000}") long delayMillis) {
        if (secret == null || !secret.matches("[!-~]{32,256}") || maxResults < 100 || delayMillis < 0 || delayMillis > 30000)
            throw new IllegalArgumentException("Invalid customer result simulator settings");
        this.secret = ("Bearer " + secret).getBytes(StandardCharsets.UTF_8);
        this.maxResults = maxResults; this.delayMillis = delayMillis;
    }

    @PostMapping("/api/v1/customer-results/{tenantId}/{mode}")
    public ResponseEntity<Void> receive(@PathVariable long tenantId, @PathVariable Mode mode, HttpServletRequest request) throws IOException {
        String auth = request.getHeader("Authorization");
        if (auth == null || !MessageDigest.isEqual(secret, auth.getBytes(StandardCharsets.UTF_8))) return ResponseEntity.status(401).build();
        byte[] bytes = request.getInputStream().readNBytes(1_048_577);
        if (bytes.length > 1_048_576) return ResponseEntity.status(413).build();
        CustomerResultBatch batch;
        try {
            batch = mapper.readValue(bytes, CustomerResultBatch.class);
            if (batch.schemaVersion() != 1 || batch.tenantId() != tenantId || tenantId <= 0
                    || !UUID.fromString(batch.batchId()).toString().equals(batch.batchId())
                    || !batch.batchId().equals(request.getHeader("Idempotency-Key"))
                    || batch.results().isEmpty() || batch.results().size() > 100
                    || batch.results().stream().anyMatch(r -> r.tenantId() != tenantId || r.eventId() == null)
                    || batch.results().stream().map(r -> r.eventId()).distinct().count() != batch.results().size()) {
                return ResponseEntity.badRequest().build();
            }
        } catch (RuntimeException invalid) { return ResponseEntity.badRequest().build(); }
        int code = record(batch, bytes, mode);
        if (mode == Mode.DELAY_ACK && code == 204) {
            try { Thread.sleep(delayMillis); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); return ResponseEntity.status(503).build(); }
        }
        return ResponseEntity.status(code).build();
    }

    private synchronized int record(CustomerResultBatch batch, byte[] bytes, Mode mode) {
        String digest;
        try { digest = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch (java.security.NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
        String key = batch.tenantId() + ":" + batch.batchId();
        var existing = batches.get(key);
        if (existing != null && !existing.digest().equals(digest)) return 409;
        if (existing == null && batches.size() >= maxResults) return 503;
        long attempt = existing == null ? 1 : existing.requests() + 1;
        var newIds = batch.results().stream().map(r -> batch.tenantId() + ":" + r.eventId()).filter(id -> !results.contains(id)).toList();
        if (results.size() + newIds.size() > maxResults) return 503;
        batches.put(key, new Seen(digest, attempt)); requests++;
        if (mode == Mode.REJECT || (mode == Mode.FAIL_ONCE && attempt == 1)) return 503;
        results.addAll(newIds);
        return mode == Mode.ACK_THEN_503 ? 503 : 204;
    }

    public synchronized Map<String, Long> summary() {
        return Map.of("requests", requests, "batches", (long) batches.size(), "uniqueResultEffects", (long) results.size(), "maxResults", (long) maxResults);
    }
}
