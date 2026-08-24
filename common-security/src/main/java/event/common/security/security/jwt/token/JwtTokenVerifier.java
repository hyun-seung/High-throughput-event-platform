package event.common.security.security.jwt.token;

import event.common.security.security.jwt.config.JwtProperties;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.io.Decoders;
import io.jsonwebtoken.security.Keys;
import org.apache.commons.lang3.StringUtils;

import javax.crypto.SecretKey;

public class JwtTokenVerifier {

    private final JwtParser jwtParser;

    public JwtTokenVerifier(JwtProperties properties) {
        SecretKey secretKey = createSecretKey(properties.secret());
        this.jwtParser = Jwts.parser().verifyWith(secretKey).build();
    }

    public Claims parseClaims(String token) {
        return jwtParser.parseSignedClaims(token).getPayload();
    }

    public Long getUserId(String token) {
        return Long.valueOf(parseClaims(token).getSubject());
    }

    private SecretKey createSecretKey(String secret) {
        if (StringUtils.isBlank(secret)) {
            throw new IllegalArgumentException("JWT secret must not be blank.");
        }

        return Keys.hmacShaKeyFor(Decoders.BASE64.decode(secret));
    }
}