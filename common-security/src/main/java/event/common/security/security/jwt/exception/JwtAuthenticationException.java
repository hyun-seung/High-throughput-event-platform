package event.common.security.security.jwt.exception;

import lombok.Getter;
import org.springframework.security.core.AuthenticationException;

@Getter
public class JwtAuthenticationException extends AuthenticationException {

    private final JwtErrorCode errorCode;

    public JwtAuthenticationException(JwtErrorCode errorCode) {
        super(errorCode.getDesc());
        this.errorCode = errorCode;
    }

    public JwtAuthenticationException(JwtErrorCode errorCode, Throwable cause) {
        super(errorCode.getDesc(), cause);
        this.errorCode = errorCode;
    }
}