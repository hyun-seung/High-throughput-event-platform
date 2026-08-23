package event.auth.api.auth.service;

import event.auth.api.auth.dto.TokenRequest;
import event.auth.api.auth.dto.TokenResponse;
import event.auth.api.auth.exception.AuthErrorCode;
import event.auth.api.auth.exception.AuthException;
import event.auth.api.auth.jwt.JwtTokenIssuer;
import event.auth.api.auth.password.PasswordMatcher;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import event.auth.api.user.entity.User;
import event.auth.api.user.entity.UserStatus;
import event.auth.api.user.repository.UserRepository;

@Service
@RequiredArgsConstructor
public class AuthService {

    private final UserRepository userRepository;
    private final PasswordMatcher passwordMatcher;
    private final JwtTokenIssuer JwtTokenIssuer;

    @Transactional
    public TokenResponse issueToken(TokenRequest request) {
        User user = userRepository.findByUsername(request.username())
                .orElseThrow(() -> new AuthException(AuthErrorCode.INVALID_CREDENTIALS));

        validateUserStatus(user);
        validatePassword(request.password(), user);

        if (!passwordMatcher.isBcrypt(user.getPassword())) {
            user.changePassword(passwordMatcher.encode(request.password()));
        }

        String accessToken = JwtTokenIssuer.createAccessToken(user);

        return TokenResponse.of(accessToken, JwtTokenIssuer.getAccessTokenExpiration());
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