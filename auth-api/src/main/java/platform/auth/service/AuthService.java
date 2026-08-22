package platform.auth.service;

import platform.auth.dto.TokenRequest;
import platform.auth.dto.TokenResponse;
import platform.auth.exception.AuthErrorCode;
import platform.auth.exception.AuthException;
import platform.auth.jwt.JwtTokenProvider;
import platform.auth.password.PasswordMatcher;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import platform.user.entity.User;
import platform.user.entity.UserStatus;
import platform.user.repository.UserRepository;

@Service
@RequiredArgsConstructor
public class AuthService {

    private final UserRepository userRepository;
    private final PasswordMatcher passwordMatcher;
    private final JwtTokenProvider jwtTokenProvider;

    @Transactional
    public TokenResponse issueToken(TokenRequest request) {
        User user = userRepository.findByUsername(request.username())
                .orElseThrow(() -> new AuthException(AuthErrorCode.INVALID_CREDENTIALS));

        validateUserStatus(user);
        validatePassword(request.password(), user);

        if (!passwordMatcher.isBcrypt(user.getPassword())) {
            user.changePassword(passwordMatcher.encode(request.password()));
        }

        String accessToken = jwtTokenProvider.createAccessToken(user);

        return TokenResponse.of(accessToken, jwtTokenProvider.getAccessTokenExpiration());
    }

    private void validateUserStatus(User user) {
        if (user.getStatus() == UserStatus.BLOCKED) {
            throw new AuthException(AuthErrorCode.USER_BLOCKED);
        }

        if (user.getStatus() == UserStatus.INACTIVE) {
            throw new AuthException(AuthErrorCode.USER_INACTIVE);
        }
    }

    private void validatePassword(String rawPassword, User user) {
        if (!passwordMatcher.matches(rawPassword, user.getPassword())) {
            throw new AuthException(AuthErrorCode.INVALID_CREDENTIALS);
        }
    }
}