package event.auth.module.auth.exception;

import event.common.core.response.ApiError;
import event.common.core.response.ApiResponse;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@Slf4j
@RestControllerAdvice
public class AuthExceptionHandler {

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

        ApiResponse<ApiError> response = ApiResponse.error(
                errorCode.getHttpStatus().value(),
                errorCode.getCode(),
                errorCode.getDesc()
        );

        return ResponseEntity.status(errorCode.getHttpStatus()).body(response);
    }
}