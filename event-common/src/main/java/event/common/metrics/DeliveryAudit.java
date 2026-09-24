package event.common.metrics;

import event.common.delivery.DeliveryEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Operational observations only. Never a durable ledger; never logs payloads or credentials. */
public final class DeliveryAudit {
    private static final Logger LOG = LoggerFactory.getLogger("delivery.audit");

    private DeliveryAudit() { }

    public static void record(DeliveryEvent event, String stage, String outcome,
                              String attemptId, String provider, String code) {
        LOG.atInfo().addKeyValue("stage", stage).addKeyValue("outcome", outcome)
                .addKeyValue("deliveryId", event.deliveryId()).addKeyValue("tenantId", event.tenantId())
                .addKeyValue("attemptId", attemptId).addKeyValue("provider", provider)
                .addKeyValue("code", code).log("Delivery stage observed");
    }
}
