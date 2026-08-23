package event.common.security.security.jwt.token;

import event.common.security.security.jwt.exception.JwtAuthenticationException;
import event.common.security.security.jwt.exception.JwtErrorCode;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.springframework.stereotype.Component;

@Component
public class JwtHeaderTokenExtractor {

    private static final String TOKEN_PREFIX = "Bearer ";

    public String extract(String authorization) {
        if (StringUtils.isBlank(authorization)) {
            throw new JwtAuthenticationException(JwtErrorCode.MISSING_AUTHORIZATION_HEADER);
        }

        if (!Strings.CS.startsWith(authorization, TOKEN_PREFIX)) {
            throw new JwtAuthenticationException(JwtErrorCode.INVALID_TOKEN);
        }

        String token = StringUtils.trim(authorization.substring(TOKEN_PREFIX.length()));

        if (StringUtils.isBlank(token)) {
            throw new JwtAuthenticationException(JwtErrorCode.MISSING_TOKEN);
        }

        return token;
    }
}