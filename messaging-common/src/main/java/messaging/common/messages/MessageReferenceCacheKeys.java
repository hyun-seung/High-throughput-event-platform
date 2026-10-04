package messaging.common.messages;

/** Redis keys maintained from PostgreSQL reference-table CDC. */
public final class MessageReferenceCacheKeys {
    private MessageReferenceCacheKeys() { }

    public static String contract(long clientId) {
        return "message:contract:{client:" + clientId + "}";
    }

    public static String phoneCarrier(String phoneNumber) {
        return "message:phone-carrier:" + phoneNumber;
    }
}
