package messaging.common.messages;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.dns.DatagramDnsQuery;
import io.netty.handler.codec.dns.DatagramDnsQueryDecoder;
import io.netty.handler.codec.dns.DatagramDnsResponse;
import io.netty.handler.codec.dns.DatagramDnsResponseEncoder;
import io.netty.handler.codec.dns.DefaultDnsRawRecord;
import io.netty.handler.codec.dns.DnsQuestion;
import io.netty.handler.codec.dns.DnsRecordType;
import io.netty.handler.codec.dns.DnsSection;
import io.netty.resolver.ResolvedAddressTypes;
import io.netty.resolver.dns.DnsAddressResolverGroup;
import io.netty.resolver.dns.DnsNameResolverBuilder;
import io.netty.resolver.dns.DnsServerAddresses;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class RedisDnsResolutionTest {
    @Test
    void boundedResolverRefreshesChangedAddressWithoutRestart() throws Exception {
        try (var dns = new ChangingDns(); var resolvers = new DnsAddressResolverGroup(
                RedisConnectionAutoConfiguration.boundCache(dns.builder(), 1))) {
            var resolver = resolvers.getResolver(dns.loops.next());
            var target = InetSocketAddress.createUnresolved("redis.test.invalid", 6379);
            var first = resolver.resolve(target).get(3, TimeUnit.SECONDS);
            dns.address.set(InetAddress.getByName("10.20.0.2"));
            assertEquals(first, resolver.resolve(target).get(3, TimeUnit.SECONDS));
            // Await expiry on the same event loop, including the scheduled cache eviction.
            dns.loops.next().schedule(() -> {}, 1200, TimeUnit.MILLISECONDS).get(3, TimeUnit.SECONDS);
            var refreshed = resolver.resolve(target).get(3, TimeUnit.SECONDS);
            assertEquals("10.20.0.2", refreshed.getAddress().getHostAddress());
            assertEquals(2, dns.queries.get());
        }
    }

    @Test
    void invalidCacheLifetimeFailsAtStartup() {
        assertThrows(IllegalArgumentException.class,
                () -> RedisConnectionAutoConfiguration.boundCache(new DnsNameResolverBuilder(), 0));
        assertThrows(IllegalArgumentException.class,
                () -> RedisConnectionAutoConfiguration.boundCache(new DnsNameResolverBuilder(), 301));
    }

    @Test
    void defaultResolverRetainsOldAddressDespiteChangedDnsAnswer() throws Exception {
        try (var dns = new ChangingDns(); var resolvers = new DnsAddressResolverGroup(dns.builder())) {
            var resolver = resolvers.getResolver(dns.loops.next());
            var target = InetSocketAddress.createUnresolved("redis.test.invalid", 6379);
            var first = resolver.resolve(target).get(3, TimeUnit.SECONDS);
            dns.address.set(InetAddress.getByName("10.20.0.2"));
            var second = resolver.resolve(target).get(3, TimeUnit.SECONDS);
            assertEquals("10.20.0.1", first.getAddress().getHostAddress());
            assertEquals(first, second);
            assertEquals(1, dns.queries.get(), "The changed DNS server was never queried again");
        }
    }

    /** Local authoritative DNS fixture with a long TTL, matching a container DNS endpoint. */
    private static final class ChangingDns implements AutoCloseable {
        final NioEventLoopGroup loops = new NioEventLoopGroup(1);
        final AtomicReference<InetAddress> address = new AtomicReference<>(InetAddress.getByName("10.20.0.1"));
        final AtomicInteger queries = new AtomicInteger();
        final Channel channel;

        ChangingDns() throws Exception {
            channel = new Bootstrap().group(loops).channel(NioDatagramChannel.class)
                    .handler(new io.netty.channel.ChannelInitializer<NioDatagramChannel>() {
                        @Override protected void initChannel(NioDatagramChannel ch) {
                            ch.pipeline().addLast(new DatagramDnsQueryDecoder(), new DatagramDnsResponseEncoder(),
                                    new SimpleChannelInboundHandler<DatagramDnsQuery>() {
                                        @Override protected void channelRead0(ChannelHandlerContext context, DatagramDnsQuery query) {
                                            DnsQuestion question = query.recordAt(DnsSection.QUESTION);
                                            var response = new DatagramDnsResponse(query.recipient(), query.sender(), query.id());
                                            response.setAuthoritativeAnswer(true).setRecursionAvailable(true);
                                            response.addRecord(DnsSection.QUESTION, question);
                                            if (question.type().equals(DnsRecordType.A)) {
                                                queries.incrementAndGet();
                                                response.addRecord(DnsSection.ANSWER, new DefaultDnsRawRecord(
                                                        question.name(), DnsRecordType.A, 600,
                                                        Unpooled.wrappedBuffer(address.get().getAddress())));
                                            }
                                            context.writeAndFlush(response);
                                        }
                                    });
                        }
                    }).bind("127.0.0.1", 0).sync().channel();
        }

        DnsNameResolverBuilder builder() {
            return new DnsNameResolverBuilder().datagramChannelType(NioDatagramChannel.class)
                    .resolvedAddressTypes(ResolvedAddressTypes.IPV4_ONLY).searchDomains(List.of())
                    .nameServerProvider(host -> DnsServerAddresses.singleton((InetSocketAddress) channel.localAddress()).stream())
                    .queryTimeoutMillis(1000);
        }

        @Override public void close() throws Exception {
            channel.close().sync();
            loops.shutdownGracefully(0, 2, TimeUnit.SECONDS).sync();
        }
    }
}
