package event.event.api.requestcontrol.exception;

import event.event.api.requestcontrol.result.RequestLimitStatus;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;

@Getter
@RequiredArgsConstructor
public enum RequestControlErrorCode {

    REQUEST_BLOCKED(HttpStatus.FORBIDDEN, 3000, "요청이 차단되었습니다."),
    TPS_LIMIT_EXCEEDED(HttpStatus.TOO_MANY_REQUESTS, 3001, "TPS 허용량을 초과했습니다."),
    MONTHLY_QUOTA_EXCEEDED(HttpStatus.TOO_MANY_REQUESTS, 3002, "월 요청 허용량을 초과했습니다."),
    POLICY_NOT_FOUND(HttpStatus.SERVICE_UNAVAILABLE, 3003, "요청 제어 정책을 찾을 수 없습니다."),
    POLICY_INVALID(HttpStatus.SERVICE_UNAVAILABLE, 3004, "요청 제어 정책이 유효하지 않습니다.");

    private final HttpStatus httpStatus;
    private final int code;
    private final String desc;

    public static RequestControlErrorCode from(RequestLimitStatus status) {
        return switch (status) {
            case BLOCKED -> REQUEST_BLOCKED;
            case TPS_LIMIT_EXCEEDED -> TPS_LIMIT_EXCEEDED;
            case MONTHLY_QUOTA_EXCEEDED -> MONTHLY_QUOTA_EXCEEDED;
            case POLICY_NOT_FOUND -> POLICY_NOT_FOUND;
            case POLICY_INVALID -> POLICY_INVALID;
            default -> throw new IllegalArgumentException("Unsupported request limit status: " + status);
        };
    }
}