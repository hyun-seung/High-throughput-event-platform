package messaging.auth.module.auth.config;

import messaging.auth.module.AuthModuleMarker;
import messaging.auth.module.auth.controller.AuthController;
import messaging.auth.module.auth.exception.AuthExceptionHandler;
import messaging.auth.module.auth.jwt.JwtTokenIssuer;
import messaging.auth.module.auth.password.PasswordMatcher;
import messaging.auth.module.auth.service.AuthService;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurationPackage;
import org.springframework.boot.data.jpa.autoconfigure.DataJpaRepositoriesAutoConfiguration;
import org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration;
import org.springframework.context.annotation.Import;

@AutoConfiguration(before = {
        HibernateJpaAutoConfiguration.class,
        DataJpaRepositoriesAutoConfiguration.class
})
@AutoConfigurationPackage(basePackageClasses = AuthModuleMarker.class)
@Import({
        PasswordConfig.class,
        PasswordMatcher.class,
        JwtTokenIssuer.class,
        AuthService.class,
        AuthController.class,
        AuthExceptionHandler.class
})
public class AuthModuleAutoConfiguration {
}
