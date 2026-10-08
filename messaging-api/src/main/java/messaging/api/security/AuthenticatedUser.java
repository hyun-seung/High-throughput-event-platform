package messaging.api.security;

public record AuthenticatedUser(
        Long userId,
        String username
) {
}