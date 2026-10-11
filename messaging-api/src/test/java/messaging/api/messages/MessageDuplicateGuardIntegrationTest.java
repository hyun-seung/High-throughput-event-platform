package messaging.api.messages;

import messaging.common.messages.MessageCategory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named = "REDIS_TEST_PORT", matches = ".+")
class MessageDuplicateGuardIntegrationTest {
    @Test
    void independentJvmsReuseTheSameRedisExecutionAndLeaveLegacyMarkersUntouched() throws Exception {
        int port = Integer.parseInt(System.getenv("REDIS_TEST_PORT"));
        long clientId = UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE;
        String messageId = "guard-" + UUID.randomUUID();
        var factory = connect(port);
        var redis = new StringRedisTemplate(factory);
        var guard = new MessageDuplicateGuard(redis, JsonMapper.builder().build());
        // Each key belongs exclusively to this test's random client and message.
        String key = guard.claim(clientId, request(messageId), "first-id").key();
        redis.delete(key);
        try {
            assertEquals("CLAIM true first-id", child(port, clientId, messageId, "first-id"));
            String marker = redis.opsForValue().get(key);
            assertNotNull(marker);
            assertTrue(marker.startsWith("first-id:v1:"));
            long ttl = redis.getExpire(key, TimeUnit.MILLISECONDS);

            assertEquals("CLAIM false first-id", child(port, clientId, messageId, "second-id"));
            assertEquals(marker, redis.opsForValue().get(key));
            assertTrue(redis.getExpire(key, TimeUnit.MILLISECONDS) <= ttl);

            String legacy = "legacy-id:" + "0".repeat(64);
            redis.opsForValue().set(key, legacy, Duration.ofMinutes(5));
            ttl = redis.getExpire(key, TimeUnit.MILLISECONDS);
            assertEquals("CLAIM false legacy-id", child(port, clientId, messageId, "third-id"));
            assertEquals(legacy, redis.opsForValue().get(key));
            long remaining = redis.getExpire(key, TimeUnit.MILLISECONDS);
            assertTrue(remaining > 0 && remaining <= ttl);
        } finally {
            redis.delete(key);
            factory.destroy();
        }
    }

    private static String child(int port, long clientId, String messageId, String proposedId) throws Exception {
        var output = Files.createTempFile("message-duplicate-", ".log");
        Process process = null;
        try {
            process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-XX:ActiveProcessorCount=2", "-Xmx128m", "-cp",
                    System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                    MessageDuplicateGuardIntegrationTest.class.getName(), Integer.toString(port),
                    Long.toString(clientId), messageId, proposedId)
                    .redirectErrorStream(true).redirectOutput(output.toFile()).start();
            assertTrue(process.waitFor(20, TimeUnit.SECONDS), "Independent JVM did not finish");
            String result = Files.readString(output, StandardCharsets.UTF_8);
            assertEquals(0, process.exitValue(), result);
            return result.lines().filter(line -> line.startsWith("CLAIM ")).findFirst().orElseThrow();
        } finally {
            if (process != null && process.isAlive()) process.destroyForcibly().waitFor();
            Files.deleteIfExists(output);
        }
    }

    private static LettuceConnectionFactory connect(int port) {
        var factory = new LettuceConnectionFactory("localhost", port);
        factory.afterPropertiesSet();
        factory.start();
        return factory;
    }

    private static MessageReceiveRequest request(String messageId) {
        return new MessageReceiveRequest(messageId, "01012345678", MessageCategory.GENERAL,
                Map.of("z", Map.of("b", 2, "a", 1), "a", "same request"), null);
    }

    public static void main(String[] args) {
        var factory = connect(Integer.parseInt(args[0]));
        try {
            var guard = new MessageDuplicateGuard(new StringRedisTemplate(factory), JsonMapper.builder().build());
            var claim = guard.claim(Long.parseLong(args[1]), request(args[2]), args[3]);
            System.out.println("CLAIM " + claim.owner() + " " + claim.clientMsgId());
        } finally {
            factory.destroy();
        }
    }
}
