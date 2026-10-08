package messaging.common.core;

public record ApiError(
        int code,
        String message
) {
}