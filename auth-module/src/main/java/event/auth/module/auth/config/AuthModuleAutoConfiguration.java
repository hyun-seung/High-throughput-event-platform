package event.auth.module.auth.config;

import event.auth.module.AuthModuleMarker;
import event.auth.module.auth.controller.AuthController;
import event.auth.module.auth.jwt.JwtTokenIssuer;
import event.auth.module.auth.password.PasswordMatcher;
import event.auth.module.auth.service.AuthService;
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
        AuthController.class
})
public class AuthModuleAutoConfiguration {
}
