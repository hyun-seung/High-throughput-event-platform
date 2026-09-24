package event.delivery.dispatch.receipt;

/** A saved receipt may need its dispatch handoff repeated until the Kafka record can complete. */
public record ReceiptApplication(String outcome, boolean resumeDispatch) { }
