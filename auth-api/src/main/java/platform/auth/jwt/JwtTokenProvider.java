package platform.auth.jwt;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.io.Decoders;
import io.jsonwebtoken.security.Keys;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import platform.user.entity.User;

import javax.crypto.SecretKey;
import java.time.Instant;
import java.util.Date;

@Component
public class JwtTokenProvider {

    private static final String CLAIM_USERNAME = "username";

    private final JwtProperties properties;
    private final SecretKey secretKey;

    public JwtTokenProvider(JwtProperties properties) {
        this.properties = properties;
        this.secretKey = createSecretKey(properties.secret());
    }

    public String createAccessToken(User user) {
        Instant now = Instant.now();
        Instant expiration = now.plusSeconds(properties.accessTokenExpiration());

        return Jwts.builder()
                .subject(String.valueOf(user.getId()))
                .claim(CLAIM_USERNAME, user.getUsername())
                .issuedAt(Date.from(now))
                .expiration(Date.from(expiration))
                .signWith(secretKey)
                .compact();
    }

    public long getAccessTokenExpiration() {
        return properties.accessTokenExpiration();
    }

    private SecretKey createSecretKey(String secret) {
        if (StringUtils.isBlank(secret)) {
            throw new IllegalArgumentException("JWT secret must not be blank.");
        }

        return Keys.hmacShaKeyFor(Decoders.BASE64.decode(secret));
    }
}