package event.delivery.dispatch.model;

public record DispatchClaim(
        DispatchClaimStatus status,
        DispatchAttempt attempt
) {

    public static DispatchClaim claimed(DispatchAttempt attempt) {
        return new DispatchClaim(DispatchClaimStatus.CLAIMED, attempt);
    }

    public static DispatchClaim alreadyAccepted() {
        return new DispatchClaim(DispatchClaimStatus.ALREADY_ACCEPTED, null);
    }

    public static DispatchClaim inProgress() {
        return new DispatchClaim(DispatchClaimStatus.IN_PROGRESS, null);
    }
}
