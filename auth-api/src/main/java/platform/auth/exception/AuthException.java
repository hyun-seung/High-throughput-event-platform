package platform.auth.exception;

import lombok.Getter;

@Getter
public class AuthException extends RuntimeException {

    private final AuthErrorCode errorCode;

    public AuthException(AuthErrorCode errorCode) {
        super(errorCode.getDesc());
        this.errorCode = errorCode;
    }
}
