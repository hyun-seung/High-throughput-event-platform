package event.receipt.api;

import event.receipt.service.ReceiptService.ReceiptAcceptanceException;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.async.AsyncRequestTimeoutException;

@RestControllerAdvice
public class ReceiptExceptionHandler {
    @ExceptionHandler({ReceiptAcceptanceException.class, AsyncRequestTimeoutException.class})
    public ResponseEntity<ErrorResponse> unconfirmed() {
        return ResponseEntity.status(503).body(new ErrorResponse("RECEIPT_UNCONFIRMED",
                "저장 여부를 확인하지 못했습니다. 동일한 receiptId와 내용으로 재전송해 주세요."));
    }

    @ExceptionHandler({MethodArgumentNotValidException.class, HttpMessageNotReadableException.class})
    public ResponseEntity<ErrorResponse> invalid() {
        return ResponseEntity.badRequest().body(new ErrorResponse("INVALID_RECEIPT", "웹훅 요청 형식을 확인해 주세요."));
    }

    public record ErrorResponse(String code, String message) { }
}
