package event.common.notification;

import event.common.lifecycle.DeliveryFinalized;
import java.util.List;

/** PoC v1: HTTP 204 acknowledges every result in this immutable batch. */
public record CustomerResultBatch(int schemaVersion, String batchId, long tenantId, List<DeliveryFinalized> results) {
    public CustomerResultBatch { results = List.copyOf(results); }
}
