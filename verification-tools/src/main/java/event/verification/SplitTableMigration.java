package event.verification;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Offline local copy from delivery_state to ORIGIN and STEP; never removes source data. */
final class SplitTableMigration {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final List<String> TABLES = List.of("ORIGIN", "STEP");

    private SplitTableMigration() {}

    static void run(String[] args) throws Exception {
        String endpoint = "http://localhost:18000";
        String source = "delivery_state";
        boolean apply = false, writersStopped = false;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--endpoint" -> endpoint = args[++i];
                case "--source-table" -> source = args[++i];
                case "--apply" -> apply = true;
                case "--writers-stopped" -> writersStopped = true;
                default -> throw new IllegalArgumentException("Usage: split-table [--endpoint local-url] [--source-table table] [--apply --writers-stopped]");
            }
        }
        URI uri = LifecycleIndexMigration.localEndpoint(endpoint);
        if (apply && !writersStopped) throw new IllegalArgumentException("--apply requires --writers-stopped");
        try (var db = DynamoDbClient.builder().endpointOverride(uri).region(Region.AP_NORTHEAST_2)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")))
                .overrideConfiguration(config -> config.apiCallTimeout(Duration.ofSeconds(15))).build()) {
            System.out.println(JSON.writeValueAsString(migrate(db, source, apply)));
        }
    }

    static String destination(Map<String, AttributeValue> item) {
        String pk = item.get("pk").s(), sk = item.get("sk").s();
        if (pk.startsWith("DELIVERY#")) {
            if (sk.equals("META")) return "ORIGIN";
            if (sk.equals("FINAL") || sk.startsWith("ATTEMPT#")) return "STEP";
        }
        if (pk.startsWith("RECEIPT#") && sk.equals("META")) return "STEP";
        throw new IllegalArgumentException("Unrecognized source record; manual mapping required");
    }

    static Map<String, Object> migrate(DynamoDbClient db, String source, boolean apply) throws Exception {
        if (TABLES.contains(source)) throw new IllegalArgumentException("Source cannot be a destination table");
        var records = new ArrayList<Record>();
        Map<String, AttributeValue> cursor = Map.of();
        do {
            var builder = software.amazon.awssdk.services.dynamodb.model.ScanRequest.builder()
                    .tableName(source).consistentRead(true);
            if (!cursor.isEmpty()) builder.exclusiveStartKey(cursor);
            var page = db.scan(builder.build());
            for (var item : page.items()) records.add(new Record(destination(item), item));
            cursor = page.lastEvaluatedKey();
        } while (cursor != null && !cursor.isEmpty());
        if (apply) ensureTables(db);
        var counts = new LinkedHashMap<String, Integer>();
        var pending = new ArrayList<Record>();
        for (var record : records) {
            counts.merge(record.table(), 1, Integer::sum);
            Map<String, AttributeValue> existing = lookup(db, record);
            if (existing == null) pending.add(record);
            else if (!canonical(existing).equals(canonical(record.item())))
                throw new IllegalStateException("Destination conflict; no existing record was overwritten");
        }
        if (apply) {
            for (var record : pending) db.putItem(request -> request.tableName(record.table()).item(record.item())
                    .conditionExpression("attribute_not_exists(pk) AND attribute_not_exists(sk)"));
            for (var record : records) {
                Map<String, AttributeValue> saved = lookup(db, record);
                if (saved == null || !canonical(saved).equals(canonical(record.item())))
                    throw new IllegalStateException("Post-copy verification failed; keep writers stopped");
            }
        }
        var summary = new LinkedHashMap<String, Object>();
        summary.put("source", source);
        summary.put("sourceItems", records.size());
        summary.put("destinations", counts);
        summary.put("missingBefore", pending.size());
        summary.put("applied", apply);
        summary.put("sourceDeleted", false);
        return summary;
    }

    private record Record(String table, Map<String, AttributeValue> item) {}

    private static Map<String, AttributeValue> lookup(DynamoDbClient db, Record record) {
        try {
            var response = db.getItem(request -> request.tableName(record.table())
                    .key(Map.of("pk", record.item().get("pk"), "sk", record.item().get("sk"))).consistentRead(true));
            return response.hasItem() ? response.item() : null;
        } catch (ResourceNotFoundException missingTable) {
            return null;
        }
    }

    static void ensureTables(DynamoDbClient db) throws Exception {
        for (String table : TABLES) {
            TableDescription description;
            try {
                description = db.describeTable(request -> request.tableName(table)).table();
            } catch (ResourceNotFoundException missing) {
                db.createTable(CreateTableRequest.builder().tableName(table).billingMode(BillingMode.PAY_PER_REQUEST)
                        .keySchema(KeySchemaElement.builder().attributeName("pk").keyType("HASH").build(),
                                KeySchemaElement.builder().attributeName("sk").keyType("RANGE").build())
                        .attributeDefinitions(AttributeDefinition.builder().attributeName("pk").attributeType(ScalarAttributeType.S).build(),
                                AttributeDefinition.builder().attributeName("sk").attributeType(ScalarAttributeType.S).build(),
                                AttributeDefinition.builder().attributeName("lifecycle_bucket").attributeType(ScalarAttributeType.S).build(),
                                AttributeDefinition.builder().attributeName("lifecycle_due").attributeType(ScalarAttributeType.N).build())
                        .globalSecondaryIndexes(GlobalSecondaryIndex.builder().indexName("lifecycle_due_v1")
                                .keySchema(KeySchemaElement.builder().attributeName("lifecycle_bucket").keyType("HASH").build(),
                                        KeySchemaElement.builder().attributeName("lifecycle_due").keyType("RANGE").build())
                                .projection(Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build()).build())
                        .build());
                try (var waiter = db.waiter()) {
                    waiter.waitUntilTableExists(request -> request.tableName(table));
                }
                description = db.describeTable(request -> request.tableName(table)).table();
            }
            var keys = keySchema(description.keySchema());
            boolean indexValid = description.globalSecondaryIndexes().stream().anyMatch(index ->
                    "lifecycle_due_v1".equals(index.indexName()) && "ACTIVE".equals(index.indexStatusAsString())
                            && ProjectionType.KEYS_ONLY == index.projection().projectionType()
                            && keySchema(index.keySchema()).equals(Set.of("lifecycle_bucket:HASH", "lifecycle_due:RANGE")));
            if (!keys.equals(Set.of("pk:HASH", "sk:RANGE")) || !indexValid)
                throw new IllegalStateException("Destination table/index schema mismatch");
        }
    }

    private static Set<String> keySchema(List<KeySchemaElement> schema) {
        var result = new HashSet<String>();
        for (var key : schema) result.add(key.attributeName() + ":" + key.keyTypeAsString());
        return result;
    }

    static Map<String, Object> canonical(Map<String, AttributeValue> item) {
        var result = new LinkedHashMap<String, Object>();
        item.forEach((name, value) -> result.put(name, canonical(value)));
        return result;
    }

    private static Object canonical(AttributeValue value) {
        if (value.s() != null) return value.s();
        if (value.n() != null) return value.n();
        if (value.bool() != null) return value.bool();
        if (value.nul() != null) return value.nul();
        if (value.b() != null) return value.b();
        if (value.hasSs()) return Set.copyOf(value.ss());
        if (value.hasNs()) return Set.copyOf(value.ns());
        if (value.hasBs()) return Set.copyOf(value.bs());
        if (value.hasM()) return canonical(value.m());
        if (value.hasL()) return value.l().stream().map(SplitTableMigration::canonical).toList();
        throw new IllegalArgumentException("Unsupported DynamoDB attribute");
    }
}
