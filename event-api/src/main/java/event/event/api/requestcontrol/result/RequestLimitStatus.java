package event.event.api.requestcontrol.result;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;

@Getter
@RequiredArgsConstructor
public enum RequestLimitStatus {

    ALLOWED(0, "요청 허용"),
    BLOCKED(1, "정책에 의해 차단"),
    TPS_LIMIT_EXCEEDED(2, "TPS 제한 초과"),
    MONTHLY_QUOTA_EXCEEDED(3, "월 사용량 한도 초과"),
    POLICY_NOT_FOUND(4, "요청 제어 정책 없음"),
    POLICY_INVALID(5, "유효하지 않은 요청 제어 정책"),
    REDIS_UNAVAILABLE_BYPASS(6, "Redis 장애로 요청 제한 우회");

    private final long scriptCode;
    private final String desc;

    public static RequestLimitStatus fromScriptCode(long scriptCode) {
        return Arrays.stream(values())
                .filter(status -> status.scriptCode == scriptCode)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown request limit status code: " + scriptCode));
    }
}