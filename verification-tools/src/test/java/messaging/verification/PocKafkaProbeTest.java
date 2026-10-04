package messaging.verification;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PocKafkaProbeTest {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String REQUESTED = "delivery.requested.v1";
    private static final String DISPATCH = "delivery.dispatch-requested.v1";
    private static final String DLT = "delivery.requested.dlt.v1";

    @Test
    void snapshotKeepsUninitializedGroupsAndDltSeparate() {
        var reader = consumer();
        var ingress = consumer();
        var dispatch = consumer();
        var requested = new TopicPartition(REQUESTED, 0);
        var sent = new TopicPartition(DISPATCH, 0);
        var dlt = new TopicPartition(DLT, 0);
        when(reader.beginningOffsets(anyCollection(), any(Duration.class))).thenAnswer(call ->
                Map.of(((List<TopicPartition>) call.getArgument(0)).getFirst(), 0L));
        when(reader.endOffsets(anyCollection(), any(Duration.class))).thenAnswer(call -> {
            TopicPartition part = ((List<TopicPartition>) call.getArgument(0)).getFirst();
            return Map.of(part, part.equals(requested) ? 10L : 0L);
        });
        when(ingress.committed(any(), any(Duration.class))).thenReturn(Map.of(requested, new OffsetAndMetadata(8)));
        when(dispatch.committed(any(), any(Duration.class))).thenReturn(Map.of());
        try (var probe = new PocKafkaProbe(reader, Map.of(REQUESTED, ingress, DISPATCH, dispatch),
                Map.of(REQUESTED, List.of(requested), DISPATCH, List.of(sent), DLT, List.of(dlt)))) {
            var snapshot = probe.snapshot(false);
            assertEquals(2L, snapshot.get("lag"));
            @SuppressWarnings("unchecked") var rows = (List<Map<String, Object>>) snapshot.get("partitions");
            assertEquals(8L, rows.getFirst().get("committed"));
            assertEquals(-1001L, rows.get(1).get("committed"));
            assertNull(rows.get(2).get("committed"));
            assertNull(rows.get(2).get("lag"));
        }
    }

    @Test
    void readsExactWindowAndRejectsExpiredOrGappedEvidence() {
        var reader = consumer();
        var part = new TopicPartition(REQUESTED, 0);
        var first = new ConsumerRecord<byte[], byte[]>(REQUESTED, 0, 4, null,
                "{\"deliveryId\":\"one\"}".getBytes(StandardCharsets.UTF_8));
        var second = new ConsumerRecord<byte[], byte[]>(REQUESTED, 0, 5, null, "bad json".getBytes(StandardCharsets.UTF_8));
        when(reader.poll(any(Duration.class))).thenReturn(new ConsumerRecords<>(Map.of(part, List.of(first, second))));
        try (var probe = new PocKafkaProbe(reader, Map.of(), Map.of(REQUESTED, List.of(part)))) {
            var before = JSON.readTree("{\"partitions\":[{\"topic\":\"" + REQUESTED + "\",\"partition\":0,\"end\":4}]}");
            var after = JSON.readTree("{\"partitions\":[{\"topic\":\"" + REQUESTED + "\",\"partition\":0,\"start\":0,\"end\":6}]}");
            var rows = probe.records(before, after);
            assertEquals(2, rows.size());
            assertEquals("one", rows.getFirst().get("deliveryId"));
            assertNull(rows.get(1).get("deliveryId"));
            var expired = JSON.readTree("{\"partitions\":[{\"topic\":\"" + REQUESTED + "\",\"partition\":0,\"start\":5,\"end\":6}]}");
            assertThrows(IllegalStateException.class, () -> probe.records(before, expired));
        }

        var gapReader = consumer();
        var gap = new ConsumerRecord<byte[], byte[]>(REQUESTED, 0, 5, null, "{}".getBytes(StandardCharsets.UTF_8));
        when(gapReader.poll(any(Duration.class))).thenReturn(new ConsumerRecords<>(Map.of(part, List.of(gap))));
        try (var probe = new PocKafkaProbe(gapReader, Map.of(), Map.of(REQUESTED, List.of(part)))) {
            var before = JSON.readTree("{\"partitions\":[{\"topic\":\"" + REQUESTED + "\",\"partition\":0,\"end\":4}]}");
            var after = JSON.readTree("{\"partitions\":[{\"topic\":\"" + REQUESTED + "\",\"partition\":0,\"start\":0,\"end\":6}]}");
            assertThrows(IllegalStateException.class, () -> probe.records(before, after));
        }
    }

    @SuppressWarnings("unchecked")
    private static KafkaConsumer<byte[], byte[]> consumer() { return mock(KafkaConsumer.class); }
}
