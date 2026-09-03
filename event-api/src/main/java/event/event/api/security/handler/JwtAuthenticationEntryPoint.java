package event.event.api.security.handler;

import event.common.core.response.ApiError;
import event.common.core.response.ApiResponse;
import event.common.security.security.jwt.exception.JwtAuthenticationException;
import event.common.security.security.jwt.exception.JwtErrorCode;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Slf4j
@Component
@RequiredArgsConstructor
public class JwtAuthenticationEntryPoint implements AuthenticationEntryPoint {

    private final JsonMapper jsonMapper;

    @Override
    public void commence(
            HttpServletRequest request,
            HttpServletResponse response,
            AuthenticationException exception
    ) throws IOException {

        JwtErrorCode errorCode = getErrorCode(exception);

        log.warn("Authentication entry point. method={}, uri={}, exception={}, message={}, code={}",
                request.getMethod(), request.getRequestURI(), exception.getClass().getSimpleName(), exception.getMessage(), errorCode.getCode());

        ApiResponse<ApiError> body = ApiResponse.error(
                errorCode.getHttpStatus().value(),
                errorCode.getCode(),
                errorCode.getDesc()
        );

        response.setStatus(errorCode.getHttpStatus().value());
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());

        jsonMapper.writeValue(response.getOutputStream(), body);
    }

    private JwtErrorCode getErrorCode(AuthenticationException exception) {
        if (exception instanceof JwtAuthenticationException jwtException) {
            return jwtException.getErrorCode();
        }

        return JwtErrorCode.AUTHENTICATION_FAILED;
    }
}