package event.event.api.requestcontrol.handler;

import event.common.core.response.ApiError;
import event.common.core.response.ApiResponse;
import event.event.api.requestcontrol.exception.RequestControlErrorCode;
import event.event.api.requestcontrol.result.RequestLimitResult;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Slf4j
@Component
@RequiredArgsConstructor
public class RequestControlFailureHandler {

    private final JsonMapper jsonMapper;

    public void handle(
            HttpServletRequest request,
            HttpServletResponse response,
            RequestLimitResult result
    ) throws IOException {

        RequestControlErrorCode errorCode = RequestControlErrorCode.from(result.status());

        log.warn(
                "Request rejected. method={}, uri={}, status={}, code={}, remainingTokens={}, monthlyUsage={}, monthlyLimit={}",
                request.getMethod(),
                request.getRequestURI(),
                result.status(),
                errorCode.getCode(),
                result.remainingTokens(),
                result.monthlyUsage(),
                result.monthlyLimit()
        );

        response.setStatus(errorCode.getHttpStatus().value());
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());

        ApiResponse<ApiError> body = new ApiResponse<>(
                errorCode.getHttpStatus().value(),
                new ApiError(errorCode.getCode(), errorCode.getDesc())
        );

        jsonMapper.writeValue(response.getWriter(), body);
    }
}