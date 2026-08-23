package event.event.api.requestcontrol.result;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum RequestLimitStatus {

    ALLOWED("요청 허용"),
    BLOCKED("정책에 의해 차단"),
    TPS_LIMIT_EXCEEDED("TPS 제한 초과"),
    MONTHLY_QUOTA_EXCEEDED("월 사용량 한도 초과"),
    POLICY_NOT_FOUND("요청 제어 정책 없음");

    private final String desc;
}