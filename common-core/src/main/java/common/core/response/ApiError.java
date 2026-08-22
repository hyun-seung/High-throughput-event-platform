package common.core.response;

public record ApiError(
        int code,
        String message
) {
}