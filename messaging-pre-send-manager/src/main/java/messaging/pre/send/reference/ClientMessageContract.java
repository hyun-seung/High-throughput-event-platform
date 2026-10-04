package messaging.pre.send.reference;

/** The contract fields currently available to PRE-SEND-MANAGER. */
public record ClientMessageContract(long clientId, boolean enabled) {
    public ClientMessageContract {
        if (clientId <= 0) throw new IllegalArgumentException("Invalid client ID");
    }
}
