package event.event.api.security.filter;

import event.common.security.security.jwt.exception.JwtAuthenticationException;
import event.common.security.security.jwt.exception.JwtErrorCode;
import event.common.security.security.jwt.token.JwtHeaderTokenExtractor;
import event.common.security.security.jwt.token.JwtTokenVerifier;
import event.event.api.security.handler.JwtAuthenticationEntryPoint;
import event.event.api.security.principal.AuthenticatedUser;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.JwtException;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Collections;

@Slf4j
@Component
@RequiredArgsConstructor
public class JwtTokenAuthenticationProcessingFilter extends OncePerRequestFilter {

    private static final String CLAIM_USERNAME = "username";

    private final JwtHeaderTokenExtractor tokenExtractor;
    private final JwtTokenVerifier jwtTokenVerifier;
    private final JwtAuthenticationEntryPoint authenticationEntryPoint;

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain
    ) throws ServletException, IOException {

        try {
            authenticate(request);
        } catch (ExpiredJwtException e) {
            // JWT 만료
            log.warn("JWT authentication failed. reason=TOKEN_EXPIRED, method={}, uri={}",
                    request.getMethod(), request.getRequestURI());

            authenticationEntryPoint.commence(request, response, new JwtAuthenticationException(JwtErrorCode.TOKEN_EXPIRED, e));
            return;
        } catch (JwtAuthenticationException e) {
            // Authorization Header 누락, Token 누락 등 인증 처리 중 발생한 명시적 예외
            log.warn("JWT authentication failed. reason={}, method={}, uri={}", e.getErrorCode(), request.getMethod(), request.getRequestURI());
            authenticationEntryPoint.commence(request, response, e);
            return;
        } catch (JwtException | IllegalArgumentException e) {
            // JWT 서명 오류, 형식 오류, Claims 파싱 오류 등 유효하지 않은 Token
            log.warn("JWT authentication failed. reason=INVALID_TOKEN, method={}, uri={}", request.getMethod(), request.getRequestURI());
            authenticationEntryPoint.commence(request, response, new JwtAuthenticationException(JwtErrorCode.INVALID_TOKEN, e));
            return;
        }

        filterChain.doFilter(request, response);
    }

    private void authenticate(HttpServletRequest request) {
        String authorization = request.getHeader(HttpHeaders.AUTHORIZATION);
        String token = tokenExtractor.extract(authorization);
        Claims claims = jwtTokenVerifier.parseClaims(token);

        String subject = claims.getSubject();
        if (StringUtils.isBlank(subject)) {
            throw new JwtAuthenticationException(JwtErrorCode.INVALID_TOKEN);
        }

        Long userId = Long.valueOf(subject);
        String username = claims.get(CLAIM_USERNAME, String.class);

        AuthenticatedUser principal = new AuthenticatedUser(userId, username);

        UsernamePasswordAuthenticationToken authentication =
                UsernamePasswordAuthenticationToken.authenticated(principal, null, Collections.emptyList());

        SecurityContext context = SecurityContextHolder.createEmptyContext();
        context.setAuthentication(authentication);
        SecurityContextHolder.setContext(context);
    }
}