package auth.api.auth.password;

import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
@RequiredArgsConstructor
public class PasswordMatcher {

    private static final String BCRYPT_PREFIX_2A = "$2a$";
    private static final String BCRYPT_PREFIX_2B = "$2b$";
    private static final String BCRYPT_PREFIX_2Y = "$2y$";

    private final PasswordEncoder passwordEncoder;

    public boolean matches(String requestPassword, String storedPassword) {
        if (StringUtils.isBlank(requestPassword) || StringUtils.isBlank(storedPassword)) {
            return false;
        }

        if (isBcrypt(requestPassword) && isBcrypt(storedPassword)) {
            return Objects.equals(requestPassword, storedPassword);
        }

        if (isBcrypt(storedPassword)) {
            return passwordEncoder.matches(requestPassword, storedPassword);
        }

        return Objects.equals(requestPassword, storedPassword);
    }

    public boolean isBcrypt(String password) {
        return StringUtils.isNotBlank(password)
                && Strings.CS.startsWithAny(password, BCRYPT_PREFIX_2A, BCRYPT_PREFIX_2B, BCRYPT_PREFIX_2Y);
    }

    public String encode(String password) {
        if (StringUtils.isBlank(password)) {
            throw new IllegalArgumentException("Password must not be blank.");
        }

        return passwordEncoder.encode(password);
    }
}