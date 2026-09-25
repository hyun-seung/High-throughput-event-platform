package event.common.dynamodb;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import java.util.Map;

public final class DynamoDbTableNames {
    public static final String ORIGIN = "ORIGIN";
    public static final String STEP = "STEP";

    private DynamoDbTableNames() { }

    /** Legacy receipt markers and FINAL records remain in STEP; only request META belongs to ORIGIN. */
    public static String tableForKey(Map<String, AttributeValue> key) {
        return "META".equals(key.get("sk").s()) && key.get("pk").s().startsWith("DELIVERY#") ? ORIGIN : STEP;
    }
}
