package event.common.security.security.jwt.config;

import event.common.security.security.jwt.token.JwtHeaderTokenExtractor;
import event.common.security.security.jwt.token.JwtTokenVerifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
@EnableConfigurationProperties(JwtProperties.class)
public class JwtAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public JwtHeaderTokenExtractor jwtHeaderTokenExtractor() {
        return new JwtHeaderTokenExtractor();
    }

    @Bean
    @ConditionalOnMissingBean
    public JwtTokenVerifier jwtTokenVerifier(JwtProperties jwtProperties) {
        return new JwtTokenVerifier(jwtProperties);
    }
}