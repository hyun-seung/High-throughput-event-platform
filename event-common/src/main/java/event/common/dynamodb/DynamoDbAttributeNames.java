package event.common.dynamodb;

public final class DynamoDbAttributeNames {

    public static final String EVENT_ID = "event_id";
    public static final String SOURCE = "source";
    public static final String USER_ID = "user_id";
    public static final String EVENT_TYPE = "event_type";
    public static final String PAYLOAD = "payload";
    public static final String OCCURRED_AT = "occurred_at";

    public static final String PK = "pk";
    public static final String SK = "sk";

    public static final String STATUS = "status";
    public static final String PUBLISH_STATUS = "publish_status";

    public static final String RECOVERY_BUCKET = "recovery_bucket";
    public static final String RECOVERY_AT = "recovery_at";

    public static final String RETRY_COUNT = "retry_count";
    public static final String CREATED_AT = "created_at";
    public static final String UPDATED_AT = "updated_at";

    private DynamoDbAttributeNames() {
    }
}