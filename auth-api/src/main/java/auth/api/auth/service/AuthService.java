package auth.api.auth.service;

import auth.api.auth.dto.TokenRequest;
import auth.api.auth.dto.TokenResponse;
import auth.api.auth.exception.AuthErrorCode;
import auth.api.auth.exception.AuthException;
import auth.api.auth.jwt.JwtTokenProvider;
import auth.api.auth.password.PasswordMatcher;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import auth.api.user.entity.User;
import auth.api.user.entity.UserStatus;
import auth.api.user.repository.UserRepository;

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