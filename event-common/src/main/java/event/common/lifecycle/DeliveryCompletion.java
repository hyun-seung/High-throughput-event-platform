package event.common.lifecycle;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Map;

public final class DeliveryCompletion {
    public static final String FENCE = "completion_event_id";
    public static final String FINGERPRINT = "request_fingerprint";
    public static final String TRACKING_VERSION = "receipt_tracking_version";
    public static final String RECEIPT_IDS = "receipt_marker_ids";
    private DeliveryCompletion() {}

    public static Map<String, AttributeValue> metaKey(String deliveryId) {
        return Map.of("pk", AttributeValue.fromS("DELIVERY#" + deliveryId), "sk", AttributeValue.fromS("META"));
    }
    public static boolean compacted(Map<String, AttributeValue> item) {
        return "COMPLETED".equals(item.getOrDefault("status", AttributeValue.fromS("")).s());
    }
    /** v1 includes canonical payload bytes but excludes per-arrival timestamps/correlation IDs. */
    public static String fingerprint(String deliveryId, long tenant, String type, boolean fallback, String canonicalPayload) {
        return hash("request:v1", deliveryId, Long.toString(tenant), type, Boolean.toString(fallback), canonicalPayload);
    }
    public static String hash(String... fields) {
        try {
            var digest = MessageDigest.getInstance("SHA-256");
            for (String field : fields) {
                byte[] bytes = field.getBytes(StandardCharsets.UTF_8);
                digest.update(ByteBuffer.allocate(4).putInt(bytes.length).array()); digest.update(bytes);
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (java.security.NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }
}
