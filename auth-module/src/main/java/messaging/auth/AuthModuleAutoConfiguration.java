package messaging.auth;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurationPackage;
import org.springframework.boot.data.jpa.autoconfigure.DataJpaRepositoriesAutoConfiguration;
import org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration;
import org.springframework.context.annotation.Import;

@AutoConfiguration(before = {
        HibernateJpaAutoConfiguration.class,
        DataJpaRepositoriesAutoConfiguration.class
})
@AutoConfigurationPackage(basePackageClasses = AuthModuleAutoConfiguration.class)
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
