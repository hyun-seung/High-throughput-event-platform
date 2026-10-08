package messaging.common.security.jwt;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;

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