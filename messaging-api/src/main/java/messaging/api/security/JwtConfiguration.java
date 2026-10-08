package messaging.api.security;

import org.springframework.context.annotation.Configuration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(JwtProperties.class)
public class JwtConfiguration {

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