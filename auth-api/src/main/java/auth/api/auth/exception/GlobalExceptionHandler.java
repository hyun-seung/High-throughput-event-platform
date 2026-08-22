package auth.api.auth.exception;

import common.core.exception.CommonErrorCode;
import common.core.response.ApiError;
import common.core.response.ApiResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(AuthException.class)
    public ResponseEntity<ApiResponse<ApiError>> handleAuthException(AuthException e) {
        AuthErrorCode errorCode = e.getErrorCode();
        ApiResponse<ApiError> response = ApiResponse.error(errorCode.getHttpStatus().value(), errorCode.getCode(), errorCode.getDesc());

        return ResponseEntity.status(errorCode.getHttpStatus()).body(response);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiResponse<ApiError>> handleValidationException(MethodArgumentNotValidException e) {
        String message = e.getBindingResult().getFieldErrors().stream()
                .findFirst()
                .map(error -> error.getDefaultMessage())
                .orElse(CommonErrorCode.INVALID_REQUEST.getDesc());

        ApiResponse<ApiError> response = ApiResponse.error(HttpStatus.BAD_REQUEST.value(), CommonErrorCode.INVALID_REQUEST.getCode(), message);

        return ResponseEntity.badRequest().body(response);
    }
}