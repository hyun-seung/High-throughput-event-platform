package event.delivery.ingress.dlt;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

/** Explicit partition/offset reads only: no subscription, group ID, commit or producer. */
public final class DltInspector {
    public record RawPage(long beginningOffset, long snapshotEndOffset, long nextOffset,
                          List<org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]>> records) { }

    /** Independent bounded scan. Out-of-range cursors are reported, never silently reset. */
    public static RawPage readRaw(String bootstrap, String topic, int partition, long offset, int limit) {
        validate(bootstrap, topic, partition, offset, limit);
        try (var consumer = open(bootstrap, limit)) {
            var tp = new TopicPartition(topic, partition);
            verifyPartition(consumer, tp); consumer.assign(List.of(tp));
            long beginning = consumer.beginningOffsets(List.of(tp)).get(tp);
            long end = consumer.endOffsets(List.of(tp)).get(tp);
            if (offset < beginning || offset > end) return new RawPage(beginning, end, offset, List.of());
            consumer.seek(tp, offset);
            long next = offset;
            long timeout = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            var rows = new ArrayList<org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]>>();
            while (rows.size() < limit && next < end) {
                if (System.nanoTime() >= timeout) throw new IllegalStateException("DLT intake timed out");
                for (var record : consumer.poll(Duration.ofMillis(250))) {
                    if (record.offset() >= end) break;
                    rows.add(record); next = record.offset() + 1;
                    if (rows.size() == limit) break;
                }
                if (rows.size() < limit) next = Math.min(end, consumer.position(tp));
            }
            // Retention may have moved while polling. Preserve the old cursor for investigation.
            beginning = consumer.beginningOffsets(List.of(tp)).get(tp);
            if (offset < beginning) return new RawPage(beginning, end, offset, List.of());
            return new RawPage(beginning, end, next, List.copyOf(rows));
        }
    }
    public record Page(long beginningOffset, long snapshotEndOffset, long nextOffset,
                       boolean reachedSnapshotEnd, List<DltInspection.Assessment> records) { }

    public static Page read(String bootstrap, String topic, int partition, long offset, int limit,
                            DltInspection inspection) {
        validate(bootstrap, topic, partition, offset, limit);
        try (var consumer = open(bootstrap, limit)) {
            var tp = new TopicPartition(topic, partition);
            verifyPartition(consumer, tp);
            consumer.assign(List.of(tp));
            long beginning = consumer.beginningOffsets(List.of(tp)).get(tp);
            long end = consumer.endOffsets(List.of(tp)).get(tp);
            if (offset < beginning || offset > end) throw new IllegalArgumentException("Offset outside retained DLT range");
            consumer.seek(tp, offset);
            long next = offset;
            long timeout = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            var rows = new ArrayList<DltInspection.Assessment>();
            Instant now = Instant.now();
            while (rows.size() < limit && next < end) {
                if (System.nanoTime() >= timeout) throw new IllegalStateException("DLT inspection timed out; no complete page");
                for (var record : consumer.poll(Duration.ofMillis(250))) {
                    if (record.offset() >= end) break;
                    rows.add(inspection.assess(record, now));
                    next = record.offset() + 1;
                    if (rows.size() == limit) break;
                }
                if (rows.size() < limit) next = Math.min(end, consumer.position(tp));
            }
            return new Page(beginning, end, next, next == end, List.copyOf(rows));
        }
    }

    private static void validate(String bootstrap, String topic, int partition, long offset, int limit) {
        if (bootstrap == null || !bootstrap.matches("(localhost|127\\.0\\.0\\.1):[0-9]+")
                || topic == null || !topic.matches("[A-Za-z0-9._-]{1,249}") || topic.equals(".") || topic.equals("..")
                || partition < 0 || offset < 0 || limit < 1 || limit > 100) {
            throw new IllegalArgumentException("Local broker, topic, partition>=0, offset>=0 and limit 1..100 required");
        }
    }

    private static void verifyPartition(KafkaConsumer<byte[], byte[]> consumer, TopicPartition tp) {
        if (consumer.partitionsFor(tp.topic()).stream().noneMatch(p -> p.partition() == tp.partition())) {
            throw new IllegalArgumentException("DLT partition does not exist");
        }
    }

    private static KafkaConsumer<byte[], byte[]> open(String bootstrap, int limit) {
        var config = new HashMap<String, Object>();
        config.put("bootstrap.servers", bootstrap);
        config.put("enable.auto.commit", false);
        config.put("allow.auto.create.topics", false);
        config.put("auto.offset.reset", "none");
        config.put("default.api.timeout.ms", 5000);
        config.put("request.timeout.ms", 5000);
        config.put("max.poll.records", limit);
        config.put("max.partition.fetch.bytes", 1024 * 1024);
        config.put("fetch.max.bytes", 2 * 1024 * 1024);
        config.put("client.id", "delivery-dlt-inspector");
        return new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }


    public static org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> fetchExact(
            String bootstrap, String topic, int partition, long offset) {
        validate(bootstrap, topic, partition, offset, 1);
        try (var consumer = open(bootstrap, 1)) {
            var tp = new TopicPartition(topic, partition);
            verifyPartition(consumer, tp);
            consumer.assign(List.of(tp));
            long beginning = consumer.beginningOffsets(List.of(tp)).get(tp);
            long end = consumer.endOffsets(List.of(tp)).get(tp);
            if (offset < beginning || offset >= end) throw new IllegalArgumentException("Exact DLT offset outside retained range");
            consumer.seek(tp, offset);
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(250))) {
                    if (record.offset() != offset) throw new IllegalArgumentException("Exact DLT offset is no longer retained");
                    return record;
                }
            }
            throw new IllegalStateException("Exact DLT record unavailable");
        }
    }

    public static void main(String[] args) {
        if (args.length != 7) {
            System.err.println("Usage: DltInspector <localhost:port> <dltTopic> <partition> <offset> <limit> <sourceTopic> <primaryTtl ISO-8601>");
            System.exit(2);
        }
        try {
            var inspection = new DltInspection(args[5], Duration.parse(args[6]));
            var page = read(args[0], args[1], Integer.parseInt(args[2]), Long.parseLong(args[3]), Integer.parseInt(args[4]), inspection);
            System.out.println(JsonMapper.builder().build().writeValueAsString(page));
        } catch (RuntimeException failure) {
            System.err.println("DLT inspection failed (no writes performed): " + failure.getClass().getSimpleName());
            System.exit(1);
        }
    }
}
