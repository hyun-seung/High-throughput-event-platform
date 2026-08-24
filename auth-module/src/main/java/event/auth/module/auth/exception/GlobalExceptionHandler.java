package event.auth.module.auth.exception;

import event.common.core.exception.CommonErrorCode;
import event.common.core.response.ApiError;
import event.common.core.response.ApiResponse;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(AuthException.class)
    public ResponseEntity<ApiResponse<ApiError>> handleAuthException(AuthException e, HttpServletRequest request) {
        AuthErrorCode errorCode = e.getErrorCode();

        log.warn(
                "Authentication request failed. method={}, uri={}, code={}, reason={}",
                request.getMethod(),
                request.getRequestURI(),
                errorCode.getCode(),
                errorCode.name()
        );
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