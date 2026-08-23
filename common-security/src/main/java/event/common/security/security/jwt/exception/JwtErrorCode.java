package event.common.security.security.jwt.exception;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;

@Getter
@RequiredArgsConstructor
public enum JwtErrorCode {

    MISSING_AUTHORIZATION_HEADER(HttpStatus.UNAUTHORIZED, 1010, "Authorization Header가 없습니다."),
    MISSING_TOKEN(HttpStatus.UNAUTHORIZED, 1011, "Token이 없습니다."),
    INVALID_TOKEN(HttpStatus.UNAUTHORIZED, 1012, "유효하지 않은 Token입니다."),
    TOKEN_EXPIRED(HttpStatus.UNAUTHORIZED, 1013, "Token이 만료되었습니다."),
    AUTHENTICATION_FAILED(HttpStatus.UNAUTHORIZED, 1014, "인증에 실패했습니다.");

    private final HttpStatus httpStatus;
    private final int code;
    private final String desc;
}