package messaging.common.messages;

import io.lettuce.core.SocketOptions;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.Transports;
import io.netty.channel.socket.SocketChannel;
import io.netty.resolver.dns.DefaultDnsCache;
import io.netty.resolver.dns.DefaultDnsCnameCache;
import io.netty.resolver.dns.DefaultAuthoritativeDnsServerCache;
import io.netty.resolver.dns.DnsAddressResolverGroup;
import io.netty.resolver.dns.DnsNameResolverBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.data.redis.autoconfigure.ClientResourcesBuilderCustomizer;
import org.springframework.boot.data.redis.autoconfigure.LettuceClientOptionsBuilderCustomizer;
import org.springframework.context.annotation.Bean;

import java.time.Duration;

/** Bound stale Redis addresses and unacknowledged TCP data during endpoint replacement. */
@AutoConfiguration(beforeName = "org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration")
@ConditionalOnClass(name = "io.lettuce.core.resource.ClientResources")
@ConditionalOnMissingBean(ClientResources.class)
public class RedisConnectionAutoConfiguration {
    @Bean
    LettuceClientOptionsBuilderCustomizer messagingRedisSocketCustomizer(
            @Value("${messaging.redis.tcp.user-timeout-seconds:10}") int timeoutSeconds) {
        if (timeoutSeconds < 1 || timeoutSeconds > 300) {
            throw new IllegalArgumentException("messaging.redis.tcp.user-timeout-seconds must be 1..300");
        }
        // TCP_USER_TIMEOUT requires a Linux native transport. Redis APs include epoll for both architectures.
        return builder -> builder.socketOptions(builder.build().getSocketOptions().mutate()
                .tcpUserTimeout(SocketOptions.TcpUserTimeoutOptions.builder().enable()
                        .tcpUserTimeout(Duration.ofSeconds(timeoutSeconds)).build())
                .build());
    }

    @Bean(destroyMethod = "close")
    DnsAddressResolverGroup messagingRedisDnsResolver(
            @Value("${messaging.redis.dns.max-ttl-seconds:5}") int maxTtlSeconds) {
        return new DnsAddressResolverGroup(boundCache(new DnsNameResolverBuilder()
                .datagramChannelType(Transports.datagramChannelClass())
                .socketChannelType(Transports.socketChannelClass().asSubclass(SocketChannel.class)), maxTtlSeconds));
    }

    @Bean
    ClientResourcesBuilderCustomizer messagingRedisDnsCustomizer(
            @Qualifier("messagingRedisDnsResolver") DnsAddressResolverGroup resolver) {
        return builder -> builder.addressResolverGroup(resolver);
    }

    static DnsNameResolverBuilder boundCache(DnsNameResolverBuilder builder, int maxTtlSeconds) {
        if (maxTtlSeconds < 1 || maxTtlSeconds > 300) {
            throw new IllegalArgumentException("messaging.redis.dns.max-ttl-seconds must be 1..300");
        }
        return builder.resolveCache(new DefaultDnsCache(0, maxTtlSeconds, 0))
                .cnameCache(new DefaultDnsCnameCache(0, maxTtlSeconds))
                .authoritativeDnsServerCache(new DefaultAuthoritativeDnsServerCache(0, maxTtlSeconds, null));
    }
}
