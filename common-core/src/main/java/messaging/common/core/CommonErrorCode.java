package messaging.common.core;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum CommonErrorCode {

    INVALID_REQUEST(50000, "잘못된 요청입니다."),
    INTERNAL_SERVER_ERROR(50001, "서버 내부 오류가 발생했습니다."),
    INVALID_REQUEST_BODY(50002, "요청 Body가 올바르지 않습니다.");

    private final int code;
    private final String desc;
}
