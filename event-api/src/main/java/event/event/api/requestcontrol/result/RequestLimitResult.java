package event.event.api.requestcontrol.result;

public record RequestLimitResult(
        RequestLimitStatus status,
        long remainingTokens,
        long monthlyUsage,
        long monthlyLimit
) {

    public boolean isAllowed() {
        return status == RequestLimitStatus.ALLOWED;
    }
}