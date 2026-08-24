package event.event.api.requestcontrol.redis;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum RequestLimitResultField {

    STATUS(0, "요청 제한 상태"),
    REMAINING_TOKENS(1, "잔여 토큰 수"),
    MONTHLY_USAGE(2, "월 사용량"),
    MONTHLY_LIMIT(3, "월 사용 한도");

    private final int index;
    private final String desc;

    public static int size() {
        return values().length;
    }
}