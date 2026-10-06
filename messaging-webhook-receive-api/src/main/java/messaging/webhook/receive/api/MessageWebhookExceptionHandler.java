package messaging.webhook.receive.api;

import messaging.webhook.receive.service.MessageWebhookReceiveService.PublicationUnconfirmed;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.async.AsyncRequestTimeoutException;

@Order(Ordered.HIGHEST_PRECEDENCE)
@RestControllerAdvice(assignableTypes = MessageWebhookController.class)
public class MessageWebhookExceptionHandler {
    @ExceptionHandler({IllegalArgumentException.class, HttpMessageNotReadableException.class})
    public ResponseEntity<ErrorResponse> invalid() {
        return ResponseEntity.badRequest().body(new ErrorResponse(
                "INVALID_MESSAGE_WEBHOOK", "웹훅 결과 형식과 건수를 확인해 주세요."));
    }

    @ExceptionHandler({PublicationUnconfirmed.class, AsyncRequestTimeoutException.class})
    public ResponseEntity<ErrorResponse> unconfirmed() {
        return ResponseEntity.status(503).body(new ErrorResponse(
                "MESSAGE_WEBHOOK_UNCONFIRMED", "저장 여부가 불명확합니다. 같은 결과 묶음을 재전송해 주세요."));
    }

    public record ErrorResponse(String code, String message) { }
}
