package event.common.events;

/** Redis keys maintained from PostgreSQL reference-table CDC. */
public final class EventReferenceCacheKeys {
    private EventReferenceCacheKeys() { }

    public static String contract(long clientId) {
        return "event:contract:{client:" + clientId + "}";
    }

    public static String phoneCarrier(String phoneNumber) {
        return "event:phone-carrier:" + phoneNumber;
    }
}
