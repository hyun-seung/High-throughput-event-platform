package event.common.delivery;

public final class DeliveryTopics {

    public static final String DELIVERY_REQUESTED = "delivery.requested.v1";
    public static final String DISPATCH_REQUESTED = "delivery.dispatch-requested.v1";
    public static final String RECEIPT_RECEIVED = "delivery.receipt-received.v1";
    public static final String DELIVERY_REQUESTED_DLT = "delivery.requested.dlt.v1";
    public static final String DISPATCH_REQUESTED_DLT = "delivery.dispatch-requested.dlt.v1";

    private DeliveryTopics() {
    }
}
