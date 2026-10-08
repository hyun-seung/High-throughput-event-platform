package messaging.api.messages;

import messaging.api.CommonErrorCode;
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

    /** Public admission codes are separate from HTTP status and provider failure codes. */
    public int errorCode() {
        return switch (status) {
            case BAD_REQUEST -> CommonErrorCode.INVALID_REQUEST.getCode();
            case CONFLICT -> 50003;
            case TOO_MANY_REQUESTS -> 30000;
            case SERVICE_UNAVAILABLE -> 50004;
            default -> CommonErrorCode.INTERNAL_SERVER_ERROR.getCode();
        };
    }
}
