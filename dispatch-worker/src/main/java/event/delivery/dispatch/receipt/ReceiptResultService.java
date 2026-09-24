package event.delivery.dispatch.receipt;

import event.common.receipt.ReceiptEvent;
import event.common.metrics.DeliveryMetrics;
import event.delivery.dispatch.config.DispatchProperties;
import event.delivery.dispatch.service.DispatchService;
import event.delivery.dispatch.service.SecondaryDispatchService;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Clock;

@Service
public class ReceiptResultService {
    private final ReceiptResultRepository repository;
    private final DispatchService primary;
    private final SecondaryDispatchService secondary;
    private final DispatchProperties properties;
    private final Clock clock;
    private final DeliveryMetrics metrics;

    public ReceiptResultService(ReceiptResultRepository repository, DispatchService primary, SecondaryDispatchService secondary,
                                DispatchProperties properties, Clock clock, DeliveryMetrics metrics) {
        this.repository = repository;
        this.primary = primary;
        this.secondary = secondary;
        this.properties = properties;
        this.clock = clock;
        this.metrics = metrics;
    }

    public void process(ReceiptEvent receipt) {
        metrics.measure(DeliveryMetrics.Stage.RECEIPT_PROCESS, () -> {
            var result = repository.apply(receipt, clock.instant());
            metrics.outcome(switch (result.outcome()) {
                case "delivered" -> DeliveryMetrics.Outcome.RECEIPT_DELIVERED;
                case "late_discarded" -> DeliveryMetrics.Outcome.RECEIPT_LATE_DISCARDED;
                case "stale_invocation", "closed_invocation", "retry_already_scheduled" -> DeliveryMetrics.Outcome.RECEIPT_STALE_DISCARDED;
                case "duplicate" -> DeliveryMetrics.Outcome.RECEIPT_DUPLICATE;
                default -> DeliveryMetrics.Outcome.RECEIPT_DECISION_STORED;
            });
            var audit = LoggerFactory.getLogger("delivery.audit").atInfo().addKeyValue("stage", "receipt_result")
                    .addKeyValue("eventId", receipt.eventId()).addKeyValue("outcome", result.outcome());
            if (!result.outcome().equals("late_discarded")) {
                audit.addKeyValue("deliveryId", receipt.deliveryId()).addKeyValue("attemptId", receipt.attemptId())
                        .addKeyValue("provider", receipt.provider()).addKeyValue("code", receipt.code());
            }
            audit.log("Provider receipt decision observed");
            if (!result.resumeDispatch()) return;
            var event = repository.loadDelivery(receipt.deliveryId());
            if (receipt.routeOrder() == 1) {
                if (!receipt.provider().equals(properties.provider())) {
                    throw new IllegalStateException("Receipt primary provider no longer configured; retain handoff");
                }
                primary.dispatch(event);
            } else {
                secondary.dispatch(event, repository.secondaryParent(receipt));
            }
        });
    }
}
