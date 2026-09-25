package event.delivery.ingress.dlt;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named = "KAFKA_TEST_BOOTSTRAP_SERVERS", matches = ".+")
class DltInspectorKafkaTest {
    @Test void pagesPreserveRecordsAndExistingCommittedOffset() throws Exception {
        String bootstrap = System.getenv("KAFKA_TEST_BOOTSTRAP_SERVERS");
        if (!bootstrap.matches("(localhost|127\\.0\\.0\\.1):[0-9]+")) throw new IllegalArgumentException("Local test broker required");
        String topic = "test.dlt-inspector." + UUID.randomUUID();
        String group = topic + ".existing";
        var tp = new TopicPartition(topic, 0);
        try (var admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
            try {
                try (var producer = new KafkaProducer<byte[], byte[]>(Map.of("bootstrap.servers", bootstrap, "acks", "all"),
                        new ByteArraySerializer(), new ByteArraySerializer())) {
                    for (int i = 0; i < 3; i++) {
                        var record = DltInspectionTest.record(DltInspectionTest.event(Instant.now().minusSeconds(1)));
                        producer.send(new ProducerRecord<>(topic, 0, record.key(), record.value(), record.headers())).get(10, TimeUnit.SECONDS);
                    }
                }
                // Seed an independent operational cursor. Inspection must not join or move this group.
                try (var existing = new KafkaConsumer<byte[], byte[]>(Map.of("bootstrap.servers", bootstrap,
                        "group.id", group, "enable.auto.commit", false), new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
                    existing.assign(List.of(tp));
                    existing.commitSync(Map.of(tp, new OffsetAndMetadata(1)));
                }
                var inspection = new DltInspection("source", Duration.ofHours(3));
                var first = DltInspector.read(bootstrap, topic, 0, 0, 2, inspection);
                assertEquals(List.of(0L, 1L), first.records().stream().map(DltInspection.Assessment::offset).toList());
                assertEquals(2, first.nextOffset());
                assertFalse(first.reachedSnapshotEnd());
                var second = DltInspector.read(bootstrap, topic, 0, first.nextOffset(), 2, inspection);
                assertEquals(1, second.records().size());
                assertEquals(3, second.nextOffset());
                assertTrue(second.reachedSnapshotEnd());
                assertEquals(1, admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS).get(tp).offset());
                assertEquals(3, admin.listOffsets(Map.of(tp, OffsetSpec.latest())).all().get(10, TimeUnit.SECONDS).get(tp).offset());
                assertEquals(first.records(), DltInspector.read(bootstrap, topic, 0, 0, 2, inspection).records());
                assertThrows(IllegalArgumentException.class, () -> DltInspector.read(bootstrap, topic, 0, 4, 1, inspection));
            } finally {
                admin.deleteConsumerGroups(List.of(group)).all().get(10, TimeUnit.SECONDS);
                admin.deleteTopics(List.of(topic)).all().get(10, TimeUnit.SECONDS);
            }
        }
    }
}
