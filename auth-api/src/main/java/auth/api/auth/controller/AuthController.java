package auth.api.auth.controller;

import auth.api.auth.dto.TokenRequest;
import auth.api.auth.dto.TokenResponse;
import auth.api.auth.service.AuthService;
import common.core.response.ApiResponse;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/auth")
@RequiredArgsConstructor
public class AuthController {

    private final AuthService authService;

    @PostMapping("/token")
    public ResponseEntity<ApiResponse<TokenResponse>> issueToken(@Valid @RequestBody TokenRequest request) {
        TokenResponse tokenResponse = authService.issueToken(request);
        return ResponseEntity.ok(ApiResponse.ok(tokenResponse));
    }
}