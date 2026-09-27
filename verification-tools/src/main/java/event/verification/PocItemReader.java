package event.verification;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.nio.charset.StandardCharsets;

/** Read-only projected ORIGIN/STEP evidence for the isolated PoC. */
final class PocItemReader {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String PROJECTION = "pk,sk,#s,occurred_at,updated_at,attempt_id,delivery_id";
    private static final Map<String, String> NAMES = Map.of("#s", "status");

    private PocItemReader() {}

    static void run(String endpoint) throws Exception {
        URI uri = LifecycleIndexMigration.localEndpoint(endpoint);
        var ids = new ArrayList<String>();
        for (var id : JSON.readTree(System.in)) ids.add(id.asText());
        try (var db = DynamoDbClient.builder().endpointOverride(uri).region(Region.AP_NORTHEAST_2)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local")))
                .overrideConfiguration(config -> config.apiCallTimeout(Duration.ofSeconds(15))).build()) {
            System.out.println(JSON.writeValueAsString(read(db, ids, Thread::sleep)));
        }
    }

    interface Sleeper { void sleep(long millis) throws InterruptedException; }

    static List<Map<String, Map<String, String>>> read(DynamoDbClient db, List<String> deliveries, Sleeper sleeper)
            throws InterruptedException {
        Map<String, Map<String, Map<String, String>>> saved = new LinkedHashMap<>();
        var originKeys = new ArrayList<Map<String, AttributeValue>>();
        for (String delivery : deliveries) originKeys.add(key("DELIVERY#" + delivery, "META"));
        fetch(db, "ORIGIN", originKeys, saved, sleeper);

        Set<String> executions = new TreeSet<>();
        for (String delivery : deliveries) {
            String pk = "DELIVERY#" + delivery;
            var meta = saved.get(itemKey(pk, "META"));
            executions.add(meta == null ? delivery : meta.getOrDefault("delivery_id", Map.of()).getOrDefault("S", delivery));
        }
        var stepKeys = new ArrayList<Map<String, AttributeValue>>();
        for (String execution : executions) {
            String attempt = UUID.nameUUIDFromBytes(("attempt:" + execution + ":mock-provider:1:1")
                    .getBytes(StandardCharsets.UTF_8)).toString();
            stepKeys.add(key("DELIVERY#" + execution, "ATTEMPT#" + attempt));
        }
        fetch(db, "STEP", stepKeys, saved, sleeper);
        return new ArrayList<>(saved.values());
    }

    private static void fetch(DynamoDbClient db, String table, List<Map<String, AttributeValue>> keys,
                              Map<String, Map<String, Map<String, String>>> saved, Sleeper sleeper)
            throws InterruptedException {
        for (int index = 0; index < keys.size(); index += 100) {
            var attributes = KeysAndAttributes.builder().keys(keys.subList(index, Math.min(index + 100, keys.size())))
                    .consistentRead(true).projectionExpression(PROJECTION).expressionAttributeNames(NAMES).build();
            Map<String, KeysAndAttributes> pending = Map.of(table, attributes);
            for (int retry = 0; retry < 6; retry++) {
                var response = db.batchGetItem(BatchGetItemRequest.builder().requestItems(pending).build());
                for (var item : response.responses().getOrDefault(table, List.of())) {
                    Map<String, Map<String, String>> converted = new LinkedHashMap<>();
                    for (var attribute : item.entrySet()) {
                        if (attribute.getValue().s() == null)
                            throw new IllegalStateException("Projected PoC attribute is not a string: " + attribute.getKey());
                        converted.put(attribute.getKey(), Map.of("S", attribute.getValue().s()));
                    }
                    String pk = item.get("pk").s(), sk = item.get("sk").s();
                    saved.put(itemKey(pk, sk), converted);
                }
                pending = response.unprocessedKeys();
                if (pending == null || pending.isEmpty()) break;
                if (retry == 5) throw new IllegalStateException("Unprocessed DB keys remain; reconciliation incomplete");
                sleeper.sleep(Math.min(2000, 100L << retry));
            }
        }
    }

    private static Map<String, AttributeValue> key(String pk, String sk) {
        return Map.of("pk", AttributeValue.fromS(pk), "sk", AttributeValue.fromS(sk));
    }

    private static String itemKey(String pk, String sk) { return pk + "\u0000" + sk; }
}
