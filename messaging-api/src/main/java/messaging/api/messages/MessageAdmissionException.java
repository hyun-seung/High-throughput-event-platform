package messaging.api.messages;

import org.springframework.http.HttpStatus;

public final class MessageAdmissionException extends RuntimeException {
    private final HttpStatus status;

    public MessageAdmissionException(HttpStatus status, String message) {
        super(message);
        this.status = status;
    }

    public MessageAdmissionException(HttpStatus status, String message, Throwable cause) {
        super(message, cause);
        this.status = status;
    }

    public HttpStatus status() { return status; }
}
