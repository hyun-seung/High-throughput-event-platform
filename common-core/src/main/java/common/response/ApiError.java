package common.response;

public record ApiError(
        int code,
        String message
) {
}