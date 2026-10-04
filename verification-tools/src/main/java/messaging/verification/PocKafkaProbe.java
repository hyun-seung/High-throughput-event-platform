package messaging.verification;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Persistent, read-only Kafka evidence probe for the isolated PoC runners. */
final class PocKafkaProbe implements AutoCloseable {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String REQUESTED = "delivery.requested.v1";
    private static final String DISPATCH = "delivery.dispatch-requested.v1";
    private static final String DLT = "delivery.requested.dlt.v1";
    private static final List<String> TOPICS = List.of(REQUESTED, DISPATCH, DLT);
    private static final Map<String, String> GROUPS = Map.of(REQUESTED, "delivery-ingress-worker",
            DISPATCH, "delivery-dispatch-worker");

    private final KafkaConsumer<byte[], byte[]> reader;
    private final Map<String, KafkaConsumer<byte[], byte[]>> groups = new LinkedHashMap<>();
    private final Map<String, List<TopicPartition>> partitions = new LinkedHashMap<>();

    private PocKafkaProbe(String bootstrap) {
        reader = consumer(bootstrap, "poc-read-only-" + UUID.randomUUID());
        try {
            GROUPS.forEach((topic, group) -> groups.put(topic, consumer(bootstrap, group)));
            for (String topic : TOPICS) {
                var metadata = reader.partitionsFor(topic, Duration.ofSeconds(10));
                if (metadata == null || metadata.isEmpty()) throw new IllegalStateException("Missing topic " + topic);
                partitions.put(topic, metadata.stream().map(info -> new TopicPartition(topic, info.partition()))
                        .sorted(Comparator.comparingInt(TopicPartition::partition)).toList());
            }
        } catch (Exception failure) {
            close();
            throw failure;
        }
    }

    PocKafkaProbe(KafkaConsumer<byte[], byte[]> reader, Map<String, KafkaConsumer<byte[], byte[]>> groups,
                  Map<String, List<TopicPartition>> partitions) {
        this.reader = reader;
        this.groups.putAll(groups);
        this.partitions.putAll(partitions);
    }

    static void run(String bootstrap) throws Exception {
        try (var probe = new PocKafkaProbe(bootstrap);
             var input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            System.out.println("{\"ready\":true}");
            System.out.flush();
            for (String line; (line = input.readLine()) != null;) {
                try {
                    JsonNode request = JSON.readTree(line);
                    Object answer = switch (request.path("op").asText()) {
                        case "snapshot" -> probe.snapshot(request.path("oldest").asBoolean(true));
                        case "records" -> probe.records(request.path("before"), request.path("after"));
                        case "close" -> Map.of("closed", true);
                        default -> throw new IllegalArgumentException("Unknown Kafka probe operation");
                    };
                    System.out.println(JSON.writeValueAsString(answer));
                    System.out.flush();
                    if (request.path("op").asText().equals("close")) return;
                } catch (Exception failure) {
                    System.out.println(JSON.writeValueAsString(Map.of("error", failure.toString())));
                    System.out.flush();
                }
            }
        }
    }

    Map<String, Object> snapshot(boolean oldest) {
        double now = System.currentTimeMillis() / 1000d;
        var rows = new ArrayList<Map<String, Object>>();
        long totalLag = 0;
        double maxAge = 0;
        for (String topic : TOPICS) {
            var parts = partitions.get(topic);
            var beginnings = reader.beginningOffsets(parts, Duration.ofSeconds(5));
            var ends = reader.endOffsets(parts, Duration.ofSeconds(5));
            Map<TopicPartition, OffsetAndMetadata> commits = groups.containsKey(topic)
                    ? groups.get(topic).committed(new HashSet<>(parts), Duration.ofSeconds(5)) : Map.of();
            for (var part : parts) {
                long start = beginnings.get(part), end = ends.get(part);
                var commit = commits.get(part);
                Long committed = groups.containsKey(topic) ? (commit == null ? -1001L : commit.offset()) : null;
                Long effective = committed == null ? null : committed < 0 ? start : committed;
                if (effective != null && (effective < start || effective > end))
                    throw new IllegalStateException("Committed offset outside retained range; do not report zero lag");
                Long lag = effective == null ? null : end - effective;
                Double age = null;
                if (oldest && lag != null && lag > 0) {
                    reader.assign(List.of(part));
                    reader.seek(part, effective);
                    var records = reader.poll(Duration.ofSeconds(3)).records(part);
                    if (records.isEmpty() || records.getFirst().offset() != effective)
                        throw new IllegalStateException("Cannot inspect oldest uncommitted record");
                    long timestamp = records.getFirst().timestamp();
                    if (timestamp >= 0) age = Math.max(0, now - timestamp / 1000d);
                }
                var row = new LinkedHashMap<String, Object>();
                row.put("topic", topic);
                row.put("partition", part.partition());
                row.put("start", start);
                row.put("end", end);
                row.put("committed", committed);
                row.put("lag", lag);
                row.put("oldestUncommittedAgeSeconds", age);
                rows.add(row);
                if (lag != null) totalLag += lag;
                if (age != null) maxAge = Math.max(maxAge, age);
            }
        }
        return Map.of("time", now, "partitions", rows, "lag", totalLag, "oldestUncommittedAgeSeconds", maxAge);
    }

    List<Map<String, Object>> records(JsonNode before, JsonNode after) {
        Map<TopicPartition, Long> initial = new LinkedHashMap<>();
        for (JsonNode row : before.path("partitions")) initial.put(partition(row), row.path("end").asLong());
        var output = new ArrayList<Map<String, Object>>();
        for (JsonNode bounds : after.path("partitions")) {
            TopicPartition part = partition(bounds);
            long start = initial.get(part), end = bounds.path("end").asLong();
            if (start < bounds.path("start").asLong()) throw new IllegalStateException("Kafka evidence expired during the run");
            if (start == end) continue;
            reader.assign(List.of(part));
            reader.seek(part, start);
            long position = start;
            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            while (position < end) {
                if (System.nanoTime() > deadline) throw new IllegalStateException("Kafka evidence collection timed out");
                var polled = reader.poll(Duration.ofSeconds(1)).records(part);
                for (var record : polled) {
                    if (record.offset() >= end) break;
                    if (record.offset() != position) throw new IllegalStateException("Kafka offset gap in test evidence");
                    position++;
                    JsonNode event;
                    try { event = record.value() == null ? JSON.readTree("{}") : JSON.readTree(record.value()); }
                    catch (Exception invalid) { event = JSON.readTree("{}"); }
                    var row = new LinkedHashMap<String, Object>();
                    row.put("topic", part.topic());
                    row.put("partition", part.partition());
                    row.put("offset", record.offset());
                    for (String key : List.of("deliveryId", "requestKey", "occurredAt")) {
                        JsonNode value = event.path(key);
                        row.put(key, value.isMissingNode() || value.isNull() ? null : value.asText());
                    }
                    output.add(row);
                }
            }
        }
        return output;
    }

    private static TopicPartition partition(JsonNode row) {
        return new TopicPartition(row.path("topic").asText(), row.path("partition").asInt());
    }

    private static KafkaConsumer<byte[], byte[]> consumer(String bootstrap, String group) {
        var config = new LinkedHashMap<String, Object>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5000);
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return new KafkaConsumer<>(config);
    }

    @Override public void close() {
        reader.close(Duration.ofSeconds(1));
        groups.values().forEach(group -> group.close(Duration.ofSeconds(1)));
    }
}
