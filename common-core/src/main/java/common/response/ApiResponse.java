package common.response;

public record ApiResponse<T>(
        int status,
        T data
) {

    public static <T> ApiResponse<T> ok(T data) {
        return new ApiResponse<>(200, data);
    }

    public static <T> ApiResponse<T> of(int status, T data) {
        return new ApiResponse<>(status, data);
    }

    public static ApiResponse<ApiError> error(int status, int code, String message) {
        return new ApiResponse<>(status, new ApiError(code, message));
    }
}