package event.auth.module.auth.exception;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;

@Getter
@RequiredArgsConstructor
public enum AuthErrorCode {

    INVALID_CREDENTIALS(HttpStatus.UNAUTHORIZED, 1000, "아이디 또는 비밀번호가 올바르지 않습니다."),
    USER_BLOCKED(HttpStatus.FORBIDDEN, 1001, "차단된 사용자입니다."),
    USER_INACTIVE(HttpStatus.FORBIDDEN, 1002, "비활성 사용자입니다."),
    TOKEN_GENERATION_FAILED(HttpStatus.INTERNAL_SERVER_ERROR, 1003, "토큰 발급 중 오류가 발생했습니다.");

    private final HttpStatus httpStatus;
    private final int code;
    private final String desc;
}