package event.event.api.security.principal;

public record AuthenticatedUser(
        Long userId,
        String username
) {
}