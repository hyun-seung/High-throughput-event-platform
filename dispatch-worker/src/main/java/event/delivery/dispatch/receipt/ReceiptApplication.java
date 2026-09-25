package event.delivery.dispatch.receipt;

import event.common.lifecycle.DeliveryFinalized;

/** A saved receipt may need its dispatch handoff repeated until the Kafka record can complete. */
public record ReceiptApplication(String outcome, boolean resumeDispatch, DeliveryFinalized finalized) {
    public ReceiptApplication(String outcome, boolean resumeDispatch) { this(outcome, resumeDispatch, null); }
}
