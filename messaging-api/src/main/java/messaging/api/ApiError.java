package messaging.api;

public record ApiError(
        int code,
        String message
) {
}