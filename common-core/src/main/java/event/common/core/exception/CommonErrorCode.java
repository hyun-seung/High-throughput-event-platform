package event.common.core.exception;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum CommonErrorCode {

    INVALID_REQUEST(9000, "잘못된 요청입니다."),
    INTERNAL_SERVER_ERROR(9001, "서버 내부 오류가 발생했습니다."),
    INVALID_REQUEST_BODY(9002, "요청 Body가 올바르지 않습니다."),
    DELIVERY_ACCEPTANCE_UNCONFIRMED(9003,
            "접수 여부를 확인하지 못했습니다. 동일한 Idempotency-Key와 요청 내용으로 재요청해 주세요.");

    private final int code;
    private final String desc;
}
