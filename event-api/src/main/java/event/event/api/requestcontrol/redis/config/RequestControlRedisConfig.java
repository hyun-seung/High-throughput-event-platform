package event.event.api.requestcontrol.redis.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.List;

@Configuration
public class RequestControlRedisConfig {

    @Bean
    public RedisScript<List> requestLimitScript() {
        return RedisScript.of(new ClassPathResource("redis/request-limit.lua"), List.class);
    }
}