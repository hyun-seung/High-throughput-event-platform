package event.auth.module.auth.dto;

public record TokenResponse(
        String accessToken,
        String tokenType,
        long expiresIn
) {

    private static final String TOKEN_TYPE = "Bearer";

    public static TokenResponse of(String accessToken, long expiresIn) {
        return new TokenResponse(accessToken, TOKEN_TYPE, expiresIn);
    }
}