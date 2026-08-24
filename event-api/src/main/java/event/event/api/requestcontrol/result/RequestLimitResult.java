package event.event.api.requestcontrol.result;

public record RequestLimitResult(
        RequestLimitStatus status,
        long remainingTokens,
        long monthlyUsage,
        long monthlyLimit
) {

    public boolean isAllowed() {
        return status == RequestLimitStatus.ALLOWED
                || status == RequestLimitStatus.REDIS_UNAVAILABLE_BYPASS;
    }

    public static RequestLimitResult redisUnavailableBypass() {
        return new RequestLimitResult(RequestLimitStatus.REDIS_UNAVAILABLE_BYPASS, -1, -1, -1);
    }
}