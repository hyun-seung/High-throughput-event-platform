package event.delivery.dispatch.external.client;

import event.common.delivery.DeliveryEvent;
import event.common.tcp.TcpDeliveryRequest;
import event.common.tcp.TcpDeliveryResponse;
import event.common.tcp.TcpFrames;
import event.delivery.dispatch.external.config.TcpProviderProperties;
import event.delivery.dispatch.external.dto.ProviderDispatchResponse;
import event.delivery.dispatch.port.SecondaryProviderClient;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import jakarta.annotation.PreDestroy;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static event.delivery.dispatch.external.client.ProviderFailureException.Kind.*;

/** Synchronous business boundary, nonblocking transport. One exchange per borrowed connection. */
@Component
@EnableConfigurationProperties(TcpProviderProperties.class)
public class TcpProviderClient implements SecondaryProviderClient, AutoCloseable {
    private final TcpProviderProperties properties;
    private final JsonMapper mapper;
    private final EventLoopGroup loops;
    private final ChannelGroup channels;
    private final FixedChannelPool pool;
    private final AtomicBoolean closed = new AtomicBoolean();

    public TcpProviderClient(TcpProviderProperties properties, JsonMapper mapper) {
        this.properties = properties;
        this.mapper = mapper;
        loops = new MultiThreadIoEventLoopGroup(properties.ioThreads(),
                Thread.ofPlatform().daemon().name("tcp-provider-", 0).factory(), NioIoHandler.newFactory());
        channels = new DefaultChannelGroup(loops.next(), true);
        var bootstrap = new Bootstrap().group(loops).channel(NioSocketChannel.class)
                .remoteAddress(properties.host(), properties.port())
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) properties.connectTimeout().toMillis())
                .option(ChannelOption.TCP_NODELAY, true).option(ChannelOption.SO_KEEPALIVE, true);
        pool = new FixedChannelPool(bootstrap, new AbstractChannelPoolHandler() {
            @Override public void channelCreated(Channel channel) {
                channels.add(channel);
                channel.pipeline().addLast(new LengthFieldBasedFrameDecoder(TcpFrames.MAX_BYTES + 4, 0, 4, 0, 4),
                        new LengthFieldPrepender(4), new ReplyHandler());
            }
        }, ChannelHealthChecker.ACTIVE, FixedChannelPool.AcquireTimeoutAction.FAIL,
                properties.acquireTimeout().toMillis(), properties.maxConnections(), properties.maxPendingAcquires());
    }

    @Override
    public ProviderDispatchResponse send(DeliveryEvent event, String idempotencyKey) {
        return send(event, idempotencyKey, 1);
    }

    @Override
    public ProviderDispatchResponse send(DeliveryEvent event, String idempotencyKey, int invocation) {
        if (closed.get()) throw new ProviderFailureException(NO_RESPONSE);
        Channel channel = null;
        boolean reusable = false;
        var reply = new CompletableFuture<byte[]>();
        try {
            byte[] body = mapper.writeValueAsBytes(TcpDeliveryRequest.from(event, idempotencyKey, invocation));
            if (body.length > TcpFrames.MAX_BYTES) throw new ProviderFailureException(PERMANENT_REJECTION);
            channel = acquire();
            Channel acquired = channel;
            channel.eventLoop().execute(() -> {
                // The waiting caller may have timed out or been interrupted before this task ran.
                if (reply.isDone() || closed.get() || !acquired.isActive()) {
                    reply.completeExceptionally(new ProviderFailureException(NO_RESPONSE));
                    return;
                }
                acquired.pipeline().get(ReplyHandler.class).pending = reply;
                acquired.writeAndFlush(Unpooled.wrappedBuffer(body)).addListener(write -> {
                    if (!write.isSuccess()) reply.completeExceptionally(new ProviderFailureException(NO_RESPONSE));
                });
            });
            var response = mapper.readValue(reply.get(properties.exchangeTimeout().toMillis(), TimeUnit.MILLISECONDS), TcpDeliveryResponse.class);
            if (response == null || !event.deliveryId().equals(response.deliveryId()) || !idempotencyKey.equals(response.attemptId())
                    || response.accepted() == null || response.processedAt() == null || response.code() == null) {
                throw new ProviderFailureException(INVALID_RESPONSE);
            }
            if (response.accepted()) {
                if (!"RECEIVED".equals(response.code())) throw new ProviderFailureException(INVALID_RESPONSE);
                reusable = true;
                return new ProviderDispatchResponse(response.deliveryId(), true, response.processedAt(), "RECEIVED");
            }
            var kind = switch (response.code()) {
                case "RETRY_1S" -> RETRY_1S;
                case "RETRY_10S" -> RETRY_10S;
                case "REJECTED" -> PERMANENT_REJECTION;
                default -> INVALID_RESPONSE;
            };
            reusable = kind != INVALID_RESPONSE;
            throw new ProviderFailureException(kind);
        } catch (JacksonException failure) {
            throw new ProviderFailureException(INVALID_RESPONSE);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new ProviderFailureException(NO_RESPONSE);
        } catch (TimeoutException failure) {
            throw new ProviderFailureException(NO_RESPONSE);
        } catch (ExecutionException failure) {
            if (failure.getCause() instanceof ProviderFailureException provider) throw provider;
            throw new ProviderFailureException(NO_RESPONSE);
        } finally {
            reply.cancel(false);
            if (channel != null) {
                // Discard uncertain streams before release: a late ACK must never satisfy another invocation.
                if (!reusable) channel.close().awaitUninterruptibly();
                pool.release(channel);
            }
        }
    }

    private Channel acquire() throws InterruptedException, ExecutionException {
        var acquisition = pool.acquire();
        try {
            return acquisition.get();
        } catch (InterruptedException interrupted) {
            // Do not cancel a pool promise and orphan a channel that connects concurrently.
            acquisition.addListener(done -> {
                if (done.isSuccess()) {
                    Channel channel = acquisition.getNow();
                    channel.close().addListener(ignored -> pool.release(channel));
                }
            });
            throw interrupted;
        }
    }

    /** All state is owned by this channel's event loop; inbound buffers are released by Netty. */
    private static final class ReplyHandler extends SimpleChannelInboundHandler<ByteBuf> {
        private CompletableFuture<byte[]> pending;

        @Override protected void channelRead0(ChannelHandlerContext ctx, ByteBuf body) {
            if (pending == null || !body.isReadable()) {
                fail(INVALID_RESPONSE);
                ctx.close();
                return;
            }
            byte[] bytes = new byte[body.readableBytes()];
            body.readBytes(bytes);
            var current = pending;
            pending = null;
            current.complete(bytes);
        }
        @Override public void channelInactive(ChannelHandlerContext ctx) { fail(NO_RESPONSE); }
        @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable failure) {
            fail(failure instanceof DecoderException ? INVALID_RESPONSE : NO_RESPONSE);
            ctx.close();
        }
        private void fail(ProviderFailureException.Kind kind) {
            if (pending != null) pending.completeExceptionally(new ProviderFailureException(kind));
            pending = null;
        }
    }

    @Override @PreDestroy
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        pool.close();
        channels.close().awaitUninterruptibly();
        loops.shutdownGracefully(0, 2, TimeUnit.SECONDS).awaitUninterruptibly();
    }
}
