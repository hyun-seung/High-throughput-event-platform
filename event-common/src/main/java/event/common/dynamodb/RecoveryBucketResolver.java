package event.common.dynamodb;

public final class RecoveryBucketResolver {

    private static final int BUCKET_COUNT = 16;
    private static final String PREFIX = "RECOVERY#";

    private RecoveryBucketResolver() {
    }

    public static String resolve(String eventId) {
        int bucket = Math.floorMod(eventId.hashCode(), BUCKET_COUNT);

        return PREFIX + String.format("%02d", bucket);
    }
}
