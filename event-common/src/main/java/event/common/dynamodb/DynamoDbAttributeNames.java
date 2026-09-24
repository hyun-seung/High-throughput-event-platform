package event.common.dynamodb;

public final class DynamoDbAttributeNames {

    public static final String EVENT_ID = "event_id";
    public static final String EVENT_TYPE = "event_type";
    public static final String DELIVERY_ID = "delivery_id";
    public static final String TENANT_ID = "tenant_id";
    public static final String DELIVERY_TYPE = "delivery_type";
    public static final String PAYLOAD = "payload";
    public static final String OCCURRED_AT = "occurred_at";
    public static final String FALLBACK_ALLOWED = "fallback_allowed";
    public static final String DEADLINE_AT = "deadline_at";
    public static final String ROUTE_ORDER = "route_order";

    public static final String PK = "pk";
    public static final String SK = "sk";

    public static final String STATUS = "status";
    public static final String ATTEMPT_ID = "attempt_id";
    public static final String PROVIDER = "provider";
    public static final String LEASE_UNTIL = "lease_until";
    public static final String VERSION = "version";
    public static final String PROVIDER_PROCESSED_AT = "provider_processed_at";
    public static final String REVIEW_REASON = "review_reason";
    public static final String RETRY_COUNT = "retry_count";
    public static final String NEXT_ATTEMPT_AT = "next_attempt_at";
    public static final String PRIMARY_DEADLINE = "primary_deadline";
    public static final String FAILURE_REASON = "failure_reason";
    public static final String FAILURE_OBSERVED_AT = "failure_observed_at";
    public static final String CREATED_AT = "created_at";
    public static final String UPDATED_AT = "updated_at";

    private DynamoDbAttributeNames() {
    }
}
