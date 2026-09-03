package event.event.api.common.exception;

import event.common.core.exception.CommonErrorCode;
import event.common.core.response.ApiError;
import event.common.core.response.ApiResponse;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.support.DefaultMessageSourceResolvable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiResponse<ApiError>> handleValidationException(
            MethodArgumentNotValidException e,
            HttpServletRequest request
    ) {
        String message = e.getBindingResult().getFieldErrors().stream()
                .findFirst()
                .map(DefaultMessageSourceResolvable::getDefaultMessage)
                .orElse(CommonErrorCode.INVALID_REQUEST.getDesc());

        log.warn("Request validation failed. method={}, uri={}, code={}",
                request.getMethod(), request.getRequestURI(), CommonErrorCode.INVALID_REQUEST.getCode());

        ApiResponse<ApiError> response = ApiResponse.error(
                HttpStatus.BAD_REQUEST.value(),
                CommonErrorCode.INVALID_REQUEST.getCode(),
                message
        );

        return ResponseEntity.badRequest().body(response);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ApiResponse<ApiError>> handleHttpMessageNotReadableException(
            HttpMessageNotReadableException e,
            HttpServletRequest request
    ) {
        CommonErrorCode errorCode = CommonErrorCode.INVALID_REQUEST_BODY;

        log.warn("Invalid request body. method={}, uri={}, code={}",
                request.getMethod(), request.getRequestURI(), errorCode.getCode());

        ApiResponse<ApiError> response = ApiResponse.error(
                HttpStatus.BAD_REQUEST.value(),
                errorCode.getCode(),
                errorCode.getDesc()
        );

        return ResponseEntity.badRequest().body(response);
    }
}