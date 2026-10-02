package event.api.events;

import org.springframework.http.HttpStatus;

public final class EventAdmissionException extends RuntimeException {
    private final HttpStatus status;

    public EventAdmissionException(HttpStatus status, String message) {
        super(message);
        this.status = status;
    }

    public EventAdmissionException(HttpStatus status, String message, Throwable cause) {
        super(message, cause);
        this.status = status;
    }

    public HttpStatus status() { return status; }
}
