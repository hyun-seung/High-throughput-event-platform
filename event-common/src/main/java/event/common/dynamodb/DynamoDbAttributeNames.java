package event.common.dynamodb;

public final class DynamoDbAttributeNames {

    public static final String EVENT_ID = "event_id";
    public static final String EVENT_TYPE = "event_type";
    public static final String DELIVERY_ID = "delivery_id";
    public static final String TENANT_ID = "tenant_id";
    public static final String DELIVERY_TYPE = "delivery_type";
    public static final String PAYLOAD = "payload";
    public static final String OCCURRED_AT = "occurred_at";

    public static final String PK = "pk";
    public static final String SK = "sk";

    public static final String STATUS = "status";
    public static final String ATTEMPT_ID = "attempt_id";
    public static final String PROVIDER = "provider";
    public static final String LEASE_UNTIL = "lease_until";
    public static final String VERSION = "version";
    public static final String PROVIDER_PROCESSED_AT = "provider_processed_at";
    public static final String REVIEW_REASON = "review_reason";
    public static final String CREATED_AT = "created_at";
    public static final String UPDATED_AT = "updated_at";

    private DynamoDbAttributeNames() {
    }
}
